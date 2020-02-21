/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2020 TeamApps.org
 * ---
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package org.teamapps.universaldb.cluster.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.cluster.message.ClusterMessage;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;

public class NodeConnection implements NetworkWriter {
	private static final Logger log = LoggerFactory.getLogger(NodeConnection.class);

	private String clusterSecret;
	private ConnectionHandler connectionHandler;
	private Socket socket;
	private boolean initialized;
	private Cipher encryptionCipher;
	private Cipher decryptionCipher;

	private byte[] random1 = new byte[8];
	private byte[] random2 = new byte[8];
	private byte[] random3 = new byte[8];
	private byte[] random4 = new byte[8];

	private volatile boolean running = true;
	private ClusterMessage initialMessage;

	private ArrayBlockingQueue<ClusterMessage> messageQueue;

	public NodeConnection(String clusterSecret, ConnectionHandler connectionHandler) {
		this.clusterSecret = clusterSecret;
		this.connectionHandler = connectionHandler;
		messageQueue = new ArrayBlockingQueue<>(100_000);
	}

	public void setSocket(Socket socket) {
		try {
			this.socket = socket;
			socket.setKeepAlive(true);
			socket.setTcpNoDelay(true);
			startReaderThread();
			startWriterThread();
		} catch (SocketException e) {
			connectionError("Error setting socket options:" + e.getMessage());
		}
	}

	public void connect(String address, int port, ClusterMessage initialMessage) {
		try {
			this.initialMessage = initialMessage;
			Socket socket = new Socket(address, port);
			this.socket = socket;
			socket.setKeepAlive(true);
			socket.setTcpNoDelay(true);
			SecureRandom secureRandom = new SecureRandom();
			secureRandom.nextBytes(random1);
			secureRandom.nextBytes(random2);
			byte[] bytes = combineByteArrays(random1, random2);
			sendMessage(MessageType.ENCRYPTION_INIT, bytes);
			startReaderThread();
			startWriterThread();
		} catch (IOException e) {
			connectionError("Error connecting to " + address + ", " + e.getMessage());
		}
	}

	public void closeConnection(String reason) {
		connectionError(reason);
	}

	public void sendMessage(ClusterMessage clusterMessage) {
		if (!messageQueue.offer(clusterMessage)){
			connectionError("Full queue - need to check cluster node state");
		}
	}

	private void sendMessage(MessageType messageType, byte[] data) {
		try {
			byte[] packet;
			if (initialized) {
				byte[] bytes = encryptionCipher.doFinal(data);
				packet = new byte[bytes.length + 5];
				ByteBuffer buffer = ByteBuffer.wrap(packet);
				buffer.put((byte) messageType.getMessageId());
				buffer.putInt(bytes.length);
				buffer.put(bytes);
			} else {
				packet = new byte[data.length + 5];
				ByteBuffer buffer = ByteBuffer.wrap(packet);
				buffer.put((byte) messageType.getMessageId());
				buffer.putInt(data.length);
				buffer.put(data);
			}
			socket.getOutputStream().write(packet);
			socket.getOutputStream().flush();
		} catch (Exception e) {
			connectionError(e.getMessage());
		}
	}

	@Override
	public void setConnectionHandler(ConnectionHandler connectionHandler) {
		this.connectionHandler = connectionHandler;
	}

	private void createCiphers() {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] ivBytes = combineByteArrays(random1, random3);
			byte[] keyInput = combineByteArrays(random2, clusterSecret.getBytes(StandardCharsets.UTF_8), random4);
			byte[] keyBytes = md.digest(keyInput);
			clusterSecret = null;
			encryptionCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			encryptionCipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(keyBytes, "AES"), new IvParameterSpec(ivBytes));
			decryptionCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			decryptionCipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(keyBytes, "AES"), new IvParameterSpec(ivBytes));
			initialized = true;
		} catch (Exception e) {
			connectionError(e.getMessage());
		}
	}

	private byte[] combineByteArrays(byte[] ... byteArrays) {
		int len = Arrays.stream(byteArrays).mapToInt(array -> array.length).sum();
		ByteArrayOutputStream bos = new ByteArrayOutputStream(len);
		for (byte[] byteArray : byteArrays) {
			bos.write(byteArray, 0, byteArray.length);
		}
		return bos.toByteArray();
	}

	private void startReaderThread()  {
		new Thread(() -> {
			try {
				boolean readHeader = true;
				byte[] packetSignature = new byte[5];
				MessageType messageType = null;
				byte[] data = null;
				int pos = 0;
				InputStream inputStream = socket.getInputStream();
				while (running) {
					if (readHeader) {
						int read = inputStream.read(packetSignature, pos, packetSignature.length - pos);
						pos += read;
						if (pos == read) {
							readHeader = false;
							ByteBuffer buffer = ByteBuffer.wrap(packetSignature);
							int messageId = buffer.get();
							messageType = MessageType.getById(messageId);
							if (messageType == null) {
								connectionError("Unknown message type:" + messageId);
							}
							int len = buffer.getInt();
							if (len <= 0 || len > 1000_000_000) {
								connectionError("Invalid message size:" + len);
								return;
							}
							data = new byte[len];
							pos = 0;
						}
					} else {
						int read = inputStream.read(data, pos, data.length - pos);
						pos += read;
						if (pos == read) {
							if (initialized) {
								byte[] bytes = decryptionCipher.doFinal(data);
								handleMessage(messageType, bytes);
							} else {
								handleMessage(messageType, data);
							}
							readHeader = true;
							pos = 0;
							messageType = null;
						}
					}
				}
			} catch (IOException e) {
				connectionError("Error reading data:" + e.getMessage());
			} catch (BadPaddingException e) {
				connectionError("Padding error:" + e.getMessage());
			} catch (IllegalBlockSizeException e) {
				connectionError("Block size error:" + e.getMessage());
			}
		}, "node-connection-reader-" + socket.getInetAddress().getHostAddress() + "-" + socket.getPort()).start();
	}

	private void startWriterThread() {
		new Thread(() -> {
			while (running) {
				try {
					ClusterMessage clusterMessage = messageQueue.take();
					sendMessage(clusterMessage.getType(), clusterMessage.getData());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}, "node-connection-writer" + socket.getInetAddress().getHostAddress() + "-" + socket.getPort()).start();
	}

	private void connectionError(String msg) {
		running = false;
		log.info("Connection error:" + msg);
		try {
			if (socket != null) {
				socket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		connectionHandler.handleConnectionError();
	}

	private void handleMessage(MessageType messageType, byte[] data) {
		if (!initialized) {
			if (messageType == MessageType.ENCRYPTION_INIT) {
				ByteBuffer buffer = ByteBuffer.wrap(data);
				buffer.get(random1);
				buffer.get(random2);
				SecureRandom secureRandom = new SecureRandom();
				secureRandom.nextBytes(random3);
				secureRandom.nextBytes(random4);
				byte[] bytes = combineByteArrays(random3, random4);
				sendMessage(MessageType.ENCRYPTION_INIT_RESPONSE, bytes);
				createCiphers();
				connectionHandler.handleConnected(this);
			} else if (messageType == MessageType.ENCRYPTION_INIT_RESPONSE) {
				ByteBuffer buffer = ByteBuffer.wrap(data);
				buffer.get(random3);
				buffer.get(random4);
				createCiphers();
				connectionHandler.handleConnected(this);
				if (initialMessage != null) {
					sendMessage(initialMessage);
				}
			}
		} else {
			connectionHandler.handleMessage(messageType, data, this);
		}
	}

}
