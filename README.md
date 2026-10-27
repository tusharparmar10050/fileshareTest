# FileShera

A clean, local-first file sharing app built with Node.js, Express, and Socket.IO. A sender creates a temporary six-digit room code; any number of receivers can join and accept the same file offer.

## Run locally

Requirements: Node.js 18.17+ and npm.

```bash
npm install
npm start
```

Open **http://localhost:3000**. To test multiple devices, open the same LAN IP and port on each device (for example `http://192.168.1.20:3000`) while all devices are on the same network. Your firewall may ask you to allow Node.js connections.

For development with Node's watch mode:

```bash
npm run dev
```

## How to use

1. On the sending device, select **Send files** and click **Create room**.
2. Share the six-digit code with your receivers.
3. On each receiving device, select **Receive files**, enter the code, and connect.
4. The sender selects a file and clicks **Send file**. Receivers accept the offer; the sender streams 64 KiB chunks to everyone who accepted.
5. Receivers click **Download** when the transfer finishes.

## Privacy and limitations

- File bytes are never written to server disk or a database. The server relays chunks in memory; the receiving browser assembles the file in memory and only saves it when the receiver clicks Download.
- This is **not end-to-end encrypted**: Socket.IO traffic uses the connection's transport security. Localhost and private LAN testing are suitable for development. For use over the public internet, deploy behind HTTPS/WSS and consider adding end-to-end encryption and stronger abuse protections.
- Six-digit codes are temporary, expire after 60 minutes, and should only be shared with intended recipients. Anyone with the code may join while the room is active.
- Default max file size is 5 GiB. Override with `MAX_FILE_BYTES` (bytes), and override the port with `PORT`.
- Large files require browser memory on the receiver. Chunking avoids loading the entire file into one sender-side buffer, but the receiver currently holds chunks until download.
- The room supports one active file transfer at a time. Start a new room to reset the session after a transfer.

## Architecture

- `server.js` — Express static server, Socket.IO room/session coordination, temporary in-memory state, chunk relay, room expiry.
- `public/index.html` — accessible sender/receiver UI.
- `public/styles.css` — responsive dark interface with lime accent.
- `public/app.js` — browser state, file selection, chunk streaming, progress, accept/decline, download.

## Security notes for a public deployment

Add HTTPS/WSS, rate limiting for room creation and code guessing, origin restrictions, per-socket event validation, file-size/transfer quotas, and an explicit consent/expiry flow. This demo does not claim that the relay server cannot inspect file bytes.
