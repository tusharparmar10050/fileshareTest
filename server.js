'use strict';

const http = require('node:http');
const crypto = require('node:crypto');
const express = require('express');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 256 * 1024,
  pingInterval: 25_000,
  pingTimeout: 20_000,
});

const PORT = Number(process.env.PORT) || 3000;
const CODE_TTL_MS = 60 * 60 * 1000;
const MAX_FILE_BYTES = Number(process.env.MAX_FILE_BYTES) || 5 * 1024 * 1024 * 1024;
const CHUNK_BYTES = 64 * 1024;
const rooms = new Map(); // In-memory only. No file bytes or metadata are written to disk.

app.disable('x-powered-by');
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('X-Frame-Options', 'DENY');
  next();
});
app.use(express.static('public', { extensions: ['html'] }));
app.get('/health', (_req, res) => res.json({ status: 'ok', activeRooms: rooms.size }));

function makeCode() {
  return String(crypto.randomInt(0, 1_000_000)).padStart(6, '0');
}
function cleanRoom(room) {
  if (!room) return;
  for (const [id, receiver] of room.receivers) {
    io.sockets.sockets.get(id)?.leave(room.code);
    io.to(id).emit('session:ended', { reason: 'The sharing session has ended.' });
  }
  io.sockets.sockets.get(room.senderId)?.leave(room.code);
  rooms.delete(room.code);
}
function publicRoomState(room) {
  return { code: room.code, receiverCount: room.receivers.size, receivers: [...room.receivers.values()].map(r => ({ id: r.id, name: r.name, status: r.status })) };
}
function sendError(ack, message) {
  if (typeof ack === 'function') ack({ ok: false, error: message });
}
function sendOk(ack, data = {}) {
  if (typeof ack === 'function') ack({ ok: true, ...data });
}

setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) if (now - room.createdAt > CODE_TTL_MS) cleanRoom(room);
}, 60_000).unref();

io.on('connection', socket => {
  socket.on('session:create', (payload = {}, ack) => {
    let code;
    do { code = makeCode(); } while (rooms.has(code));
    const room = { code, senderId: socket.id, senderName: safeName(payload.name, 'Sender'), receivers: new Map(), createdAt: Date.now(), transfer: null };
    rooms.set(code, room);
    socket.join(code);
    socket.data.role = 'sender';
    socket.data.roomCode = code;
    sendOk(ack, { code, maxFileBytes: MAX_FILE_BYTES, chunkBytes: CHUNK_BYTES });
  });

  socket.on('session:join', (payload = {}, ack) => {
    const code = String(payload.code || '').trim();
    if (!/^\d{6}$/.test(code)) return sendError(ack, 'Enter a valid 6-digit code.');
    const room = rooms.get(code);
    if (!room) return sendError(ack, 'That code is not active. Ask the sender for a new code.');
    if (room.senderId === socket.id) return sendError(ack, 'You are already the sender for this session.');
    if (room.receivers.has(socket.id)) return sendOk(ack, { ...publicRoomState(room), role: 'receiver' });
    if (room.transfer && ['active', 'complete'].includes(room.transfer.status)) return sendError(ack, 'A transfer is already underway. Ask the sender to create a new room.');
    const name = safeName(payload.name, `Receiver ${room.receivers.size + 1}`);
    room.receivers.set(socket.id, { id: socket.id, name, status: room.transfer?.status === 'offered' ? 'offered' : 'connected' });
    if (room.transfer?.status === 'offered') io.to(socket.id).emit('transfer:offer', { transferId: room.transfer.id, name: room.transfer.name, size: room.transfer.size, mimeType: room.transfer.mimeType, senderName: room.senderName });
    socket.join(code);
    socket.data.role = 'receiver';
    socket.data.roomCode = code;
    io.to(room.senderId).emit('session:state', publicRoomState(room));
    io.to(code).emit('peer:joined', { name, receiverCount: room.receivers.size });
    sendOk(ack, { ...publicRoomState(room), role: 'receiver' });
  });

  socket.on('transfer:offer', (payload = {}, ack) => {
    const room = rooms.get(socket.data.roomCode);
    if (!room || room.senderId !== socket.id) return sendError(ack, 'Only the sender can offer a file.');
    if (!room.receivers.size) return sendError(ack, 'Waiting for someone to join your code.');
    if (room.transfer && room.transfer.status === 'active') return sendError(ack, 'A transfer is already in progress.');
    const name = safeFileName(payload.name);
    const size = Number(payload.size);
    if (!name || !Number.isSafeInteger(size) || size < 1 || size > MAX_FILE_BYTES) return sendError(ack, `File must be between 1 byte and ${formatBytes(MAX_FILE_BYTES)}.`);
    const transferId = crypto.randomUUID();
    room.transfer = { id: transferId, name, size, mimeType: safeMime(payload.mimeType), status: 'offered', accepted: new Set(), startedAt: Date.now() };
    for (const receiver of room.receivers.values()) {
      receiver.status = 'offered';
      io.to(receiver.id).emit('transfer:offer', { transferId, name, size, mimeType: room.transfer.mimeType, senderName: room.senderName });
    }
    io.to(room.senderId).emit('session:state', publicRoomState(room));
    sendOk(ack, { transferId, receiverCount: room.receivers.size });
  });

  socket.on('transfer:accept', (payload = {}, ack) => {
    const room = rooms.get(socket.data.roomCode);
    const transfer = room?.transfer;
    const receiver = room?.receivers.get(socket.id);
    if (!room || !receiver || !transfer || transfer.id !== payload.transferId || transfer.status !== 'offered' || receiver.status !== 'offered') return sendError(ack, 'This file offer is no longer available.');
    transfer.accepted.add(socket.id);
    receiver.status = 'ready';
    io.to(room.senderId).emit('transfer:recipient', { id: socket.id, name: receiver.name, status: 'ready', acceptedCount: transfer.accepted.size });
    io.to(room.senderId).emit('session:state', publicRoomState(room));
    const allResponded = [...room.receivers.values()].every(r => r.status === 'ready' || r.status === 'declined');
    if (allResponded && transfer.accepted.size) { transfer.status = 'active'; io.to(room.senderId).emit('transfer:start', { transferId: transfer.id, acceptedCount: transfer.accepted.size }); }
    else if (allResponded && !transfer.accepted.size) io.to(room.senderId).emit('transfer:no-recipients', { transferId: transfer.id });
    sendOk(ack, { transferId: transfer.id });
  });

  socket.on('transfer:decline', (payload = {}) => {
    const room = rooms.get(socket.data.roomCode);
    const receiver = room?.receivers.get(socket.id);
    if (!room || !receiver) return;
    receiver.status = 'declined';
    if (room.transfer?.id === payload.transferId) {
      room.transfer.accepted.delete(socket.id);
      const allResponded = [...room.receivers.values()].every(r => r.status === 'ready' || r.status === 'declined');
      if (allResponded && room.transfer.accepted.size && room.transfer.status === 'offered') { room.transfer.status = 'active'; io.to(room.senderId).emit('transfer:start', { transferId: room.transfer.id, acceptedCount: room.transfer.accepted.size }); }
      else if (allResponded && !room.transfer.accepted.size) io.to(room.senderId).emit('transfer:no-recipients', { transferId: room.transfer.id });
    }
    io.to(room.senderId).emit('transfer:recipient', { id: socket.id, name: receiver.name, status: 'declined' });
    io.to(room.senderId).emit('session:state', publicRoomState(room));
  });

  socket.on('transfer:chunk', (payload = {}, ack) => {
    const room = rooms.get(socket.data.roomCode);
    const transfer = room?.transfer;
    if (!room || room.senderId !== socket.id || !transfer || transfer.id !== payload.transferId || transfer.status !== 'active') return sendError(ack, 'No active transfer found.');
    const index = Number(payload.index);
    const buffer = payload.chunk;
    if (!Number.isSafeInteger(index) || index < 0 || !buffer || buffer.byteLength < 1 || buffer.byteLength > CHUNK_BYTES) return sendError(ack, 'Invalid file chunk.');
    if (!transfer.accepted.size) return sendError(ack, 'No receivers have accepted this file.');
    for (const receiverId of transfer.accepted) {
      if (room.receivers.has(receiverId)) io.to(receiverId).emit('transfer:chunk', { transferId: transfer.id, index, chunk: buffer });
    }
    sendOk(ack);
  });

  socket.on('transfer:complete', (payload = {}, ack) => {
    const room = rooms.get(socket.data.roomCode);
    const transfer = room?.transfer;
    if (!room || room.senderId !== socket.id || !transfer || transfer.id !== payload.transferId) return sendError(ack, 'Transfer not found.');
    transfer.status = 'complete';
    for (const receiverId of transfer.accepted) io.to(receiverId).emit('transfer:complete', { transferId: transfer.id, name: transfer.name, size: transfer.size, mimeType: transfer.mimeType });
    for (const receiver of room.receivers.values()) receiver.status = transfer.accepted.has(receiver.id) ? 'received' : receiver.status;
    io.to(room.senderId).emit('session:state', publicRoomState(room));
    sendOk(ack);
  });

  socket.on('transfer:cancel', (payload = {}) => {
    const room = rooms.get(socket.data.roomCode);
    if (!room?.transfer || room.transfer.id !== payload.transferId) return;
    room.transfer.status = 'cancelled';
    for (const receiver of room.receivers.values()) io.to(receiver.id).emit('transfer:cancelled', { transferId: room.transfer.id });
    io.to(room.senderId).emit('transfer:cancelled', { transferId: room.transfer.id });
  });

  socket.on('session:leave', () => leaveSession(socket));
  socket.on('disconnect', () => leaveSession(socket));
});

function leaveSession(socket) {
  const code = socket.data.roomCode;
  if (!code) return;
  const room = rooms.get(code);
  socket.data.roomCode = null;
  if (!room) return;
  if (room.senderId === socket.id) {
    cleanRoom(room);
    return;
  }
  const receiver = room.receivers.get(socket.id);
  if (receiver) {
    room.receivers.delete(socket.id);
    room.transfer?.accepted.delete(socket.id);
    io.to(room.senderId).emit('session:state', publicRoomState(room));
    io.to(room.senderId).emit('peer:left', { name: receiver.name, receiverCount: room.receivers.size });
  }
  socket.leave(code);
}
function safeName(value, fallback) {
  const name = String(value || '').trim().replace(/[<>\u0000-\u001f]/g, '').slice(0, 32);
  return name || fallback;
}
function safeFileName(value) {
  return String(value || '').split(/[\\/]/).pop().replace(/[\u0000-\u001f]/g, '').trim().slice(0, 180);
}
function safeMime(value) {
  const mime = String(value || 'application/octet-stream').slice(0, 120);
  return /^[\w.+-]+\/[\w.+-]+$/.test(mime) ? mime : 'application/octet-stream';
}
function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let value = bytes / 1024, i = 0;
  while (value >= 1024 && i < units.length - 1) { value /= 1024; i++; }
  return `${value.toFixed(1)} ${units[i]}`;
}

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n  FileShera is ready → http://localhost:${PORT}\n  Max file size: ${formatBytes(MAX_FILE_BYTES)} | Chunk size: ${formatBytes(CHUNK_BYTES)}\n`);
});
