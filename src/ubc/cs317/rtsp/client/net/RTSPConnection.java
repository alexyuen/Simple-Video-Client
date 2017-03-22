/*
 * University of British Columbia
 * Department of Computer Science
 * CPSC317 - Internet Programming
 * Assignment 2
 * 
 * Author: Jonatan Schroeder
 * January 2013
 * 
 * This code may not be used without written consent of the authors, except for 
 * current and future projects and assignments of the CPSC317 course at UBC.
 */

package ubc.cs317.rtsp.client.net;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.PriorityBlockingQueue;

import ubc.cs317.rtsp.client.exception.RTSPException;
import ubc.cs317.rtsp.client.model.Frame;
import ubc.cs317.rtsp.client.model.Session;

/**
 * This class represents a connection with an RTSP server.
 */
public class RTSPConnection {

	private static final int BUFFER_LENGTH = 15000;
	private static final long MINIMUM_DELAY_READ_PACKETS_MS = 20;

	private Session session;
	private Timer rtpTimer;

	private Socket rtspSocket;
	private PrintWriter rtspWriter;
	private BufferedReader rtspReader;

	private DatagramSocket rtpSocket;

	private State state = null;
	private int curSeq = 0;
	private int sessionID;

	private static PriorityBlockingQueue<Frame> bufferedFrames = new PriorityBlockingQueue<Frame>();
	
	// statistics
	private Timer statsTimer;
	private volatile int fps = 0;
	private volatile static int packetsUnordered = 0;
	private static int expectedSeq = 0;
	private static int packetsParsed = 0;
	private static PriorityQueue<Integer> allPackets = new PriorityQueue<Integer>();
	private static PriorityQueue<Integer> lostPackets = new PriorityQueue<Integer>();
	private static ArrayList<Integer> fpsList = new ArrayList<Integer>();

	/**
	 * Establishes a new connection with an RTSP server. No message is sent at
	 * this point, and no stream is set up.
	 * 
	 * @param session
	 *            The Session object to be used for connectivity with the UI.
	 * @param server
	 *            The hostname or IP address of the server.
	 * @param port
	 *            The TCP port number where the server is listening to.
	 * @throws RTSPException
	 *             If the connection couldn't be accepted, such as if the host
	 *             name or port number are invalid or there is no connectivity.
	 */
	public RTSPConnection(Session session, String server, int port) throws RTSPException {
		this.session = session;
		try {
			rtspSocket = new Socket(server, port);
			rtspWriter = new PrintWriter(new OutputStreamWriter(rtspSocket.getOutputStream()));
			rtspReader = new BufferedReader(new InputStreamReader(rtspSocket.getInputStream()));
		} catch (Exception e) {
			throw new RTSPException(e);
		}
		state = State.INIT;
	}

	/**
	 * Sends a SETUP request to the server. This method is responsible for
	 * sending the SETUP request, receiving the response and retrieving the
	 * session identification to be used in future messages. It is also
	 * responsible for establishing an RTP datagram socket to be used for data
	 * transmission by the server. The datagram socket should be created with a
	 * random UDP port number, and the port number used in that connection has
	 * to be sent to the RTSP server for setup. This datagram socket should also
	 * be defined to timeout after 1 second if no packet is received.
	 * 
	 * @param videoName
	 *            The name of the video to be setup.
	 * @throws RTSPException
	 *             If there was an error sending or receiving the RTSP data, or
	 *             if the RTP socket could not be created, or if the server did
	 *             not return a successful response.
	 */
	public synchronized void setup(String videoName) throws RTSPException {
		if (state != State.INIT) {
			throw new RTSPException("Wrong state for setup call!");
		}
		try {
			rtpSocket = new DatagramSocket();
			rtpSocket.setSoTimeout(1000);
		} catch (SocketException e) {
			throw new RTSPException(e);
		}
		rtspWriter.println("SETUP " + videoName + " RTSP/1.0");
		rtspWriter.println("CSeq: " + ++curSeq);
		rtspWriter.println("Transport: RTP/UDP; client_port= " + rtpSocket.getLocalPort());
		rtspWriter.println();
		rtspWriter.flush();
		System.out.println("UDP Port: " + rtpSocket.getLocalPort());

		try {
			RTSPResponse response = RTSPResponse.readRTSPResponse(rtspReader);
			if (response.getResponseCode() == 200) {
				sessionID = response.getSessionId();
				state = State.READY;
			} else {
				System.out.println(response.getResponseCode());
				throw new RTSPException(response.getResponseMessage());
			}
		} catch (IOException e) {
			throw new RTSPException(e);
		}
	}

	/**
	 * Sends a PLAY request to the server. This method is responsible for
	 * sending the request, receiving the response and, in case of a successful
	 * response, starting the RTP timer responsible for receiving RTP packets
	 * with frames.
	 * 
	 * @throws RTSPException
	 *             If there was an error sending or receiving the RTSP data, or
	 *             if the server did not return a successful response.
	 */
	public synchronized void play() throws RTSPException {
		if (state != State.READY) {
			throw new RTSPException("Wrong state for play call!");
		}
		rtspWriter.println("PLAY " + session.getVideoName() + " RTSP/1.0");
		rtspWriter.println("CSeq: " + ++curSeq);
		rtspWriter.println("Session: " + sessionID);
		rtspWriter.println();
		rtspWriter.flush();

		try {
			RTSPResponse response = RTSPResponse.readRTSPResponse(rtspReader);
			if (response.getResponseCode() == 200) {
				startRTPTimer();
				state = State.PLAYING;
			} else {
				throw new RTSPException(response.getResponseMessage());
			}
		} catch (IOException e) {
			throw new RTSPException(e);
		}

	}

	/**
	 * Starts a timer that reads RTP packets repeatedly. The timer will wait at
	 * least MINIMUM_DELAY_READ_PACKETS_MS after receiving a packet to read the
	 * next one.
	 */
	private void startRTPTimer() {
		rtpTimer = new Timer();
		rtpTimer.schedule(new TimerTask() {
			@Override
			public void run() {
				receiveRTPPacket();
			}
		}, 0, MINIMUM_DELAY_READ_PACKETS_MS);
		
		
		if (statsTimer != null)
			statsTimer.cancel();
		statsTimer = new Timer(true);
		statsTimer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				//if (fps > 0)
					fpsList.add(fps);
				fps = 0;
			}
		}, 0, 1000);

	}

	/**
	 * Receives a single RTP packet and processes the corresponding frame. The
	 * data received from the datagram socket is assumed to be no larger than
	 * BUFFER_LENGTH bytes. This data is then parsed into a Frame object (using
	 * the parseRTPPacket method) and the method session.processReceivedFrame is
	 * called with the resulting packet. In case of timeout no exception should
	 * be thrown and no frame should be processed.
	 */
	private void receiveRTPPacket() {
		byte[] buffer = new byte[BUFFER_LENGTH];
		DatagramPacket dp = new DatagramPacket(buffer, buffer.length);
		try {
			rtpSocket.receive(dp);
			fps++;
			Frame result = parseRTPPacket(dp.getData());
			
			bufferedFrames.add(result);
			if (bufferedFrames.size() >= 15) {
				Frame f = bufferedFrames.poll();
				session.processReceivedFrame(f);
			}
		} catch (SocketTimeoutException e) {
			if (bufferedFrames.size() > 0) {
				while (!bufferedFrames.isEmpty()) {
					Frame f = bufferedFrames.poll();
					session.processReceivedFrame(f);
				}
			}
			int avgFps = 0;
			for (Integer i : fpsList) {
				avgFps += i;
			}
			avgFps /= fpsList.size();
			System.out.printf("Average FPS: %d, Lost packets: %d, Out-of-order packets: %d\n", avgFps, lostPackets.size(), packetsUnordered);
			System.out.println(lostPackets.toString());
			rtpTimer.cancel();
		} catch (IOException e) {
			//e.printStackTrace();
		} 
	}

	/**
	 * Sends a PAUSE request to the server. This method is responsible for
	 * sending the request, receiving the response and, in case of a successful
	 * response, cancelling the RTP timer responsible for receiving RTP packets
	 * with frames.
	 * 
	 * @throws RTSPException
	 *             If there was an error sending or receiving the RTSP data, or
	 *             if the server did not return a successful response.
	 */
	public synchronized void pause() throws RTSPException {
		if (state != State.PLAYING) {
			throw new RTSPException("Wrong state for pause call!");
		}
		rtspWriter.println("PAUSE " + session.getVideoName() + " RTSP/1.0");
		rtspWriter.println("CSeq: " + ++curSeq);
		rtspWriter.println("Session: " + sessionID);
		rtspWriter.println();
		rtspWriter.flush();

		try {
			RTSPResponse response = RTSPResponse.readRTSPResponse(rtspReader);
			if (response.getResponseCode() == 200) {
				rtpTimer.cancel();
				statsTimer.cancel();
				state = State.READY;
			} else {
				throw new RTSPException(response.getResponseMessage());
			}
		} catch (IOException e) {
			throw new RTSPException(e);
		}

	}

	/**
	 * Sends a TEARDOWN request to the server. This method is responsible for
	 * sending the request, receiving the response and, in case of a successful
	 * response, closing the RTP socket. This method does not close the RTSP
	 * connection, and a further SETUP in the same connection should be
	 * accepted. Also this method can be called both for a paused and for a
	 * playing stream, so the timer responsible for receiving RTP packets will
	 * also be cancelled.
	 * 
	 * @throws RTSPException
	 *             If there was an error sending or receiving the RTSP data, or
	 *             if the server did not return a successful response.
	 */
	public synchronized void teardown() throws RTSPException {
		if (state == State.INIT) {
			throw new RTSPException("Wrong state for teardown call!");
		}
		rtspWriter.println("TEARDOWN " + session.getVideoName() + " RTSP/1.0");
		rtspWriter.println("CSeq: " + ++curSeq);
		rtspWriter.println("Session: " + sessionID);
		rtspWriter.println();
		rtspWriter.flush();

		try {
			RTSPResponse response = RTSPResponse.readRTSPResponse(rtspReader);
			if (response.getResponseCode() == 200) {
				if (rtpTimer != null)
					rtpTimer.cancel();
				if (rtpSocket != null)
					rtpSocket.close();
				if (statsTimer != null) {
					statsTimer.cancel();
					fps = 0;
					packetsUnordered = 0;
					expectedSeq = 0;
					packetsParsed = 0;
					allPackets.clear();
					lostPackets.clear();
					fpsList.clear();
				}
				state = State.INIT;
			} else {
				throw new RTSPException(response.getResponseMessage());
			}
		} catch (IOException e) {
			throw new RTSPException(e);
		}
	}

	/**
	 * Closes the connection with the RTSP server. This method should also close
	 * any open resource associated to this connection, such as the RTP
	 * connection, if it is still open.
	 */
	public synchronized void closeConnection() {
		if (rtpTimer != null)
			rtpTimer.cancel();
		if (rtpSocket != null)
			rtpSocket.close();
		if (statsTimer != null)
			statsTimer.cancel();

		try {
			rtspWriter.close();
			rtspReader.close();
			rtspSocket.close();
		} catch (IOException e) {
		}
		state = null;
	}

	/**
	 * Parses an RTP packet into a Frame object.
	 * 
	 * @param packet
	 *            the byte representation of a frame, corresponding to the RTP
	 *            packet.
	 * @return A Frame object.
	 */
	private static Frame parseRTPPacket(byte[] packet) {
		int payloadType = (packet[1] & 0x7F);
		//System.out.println((byte) payloadType);
		if (payloadType != 26) // this payload is not a jpeg frame
			return null;

		boolean marker = (packet[1] >> 8) == 1;
		int sequence = (packet[2] << 8) | (packet[3] & 0xFF);
		int timestamp = (packet[4] << 24) | (packet[5] << 16) | (packet[6] << 8) | (packet[7] & 0xFF);
		
		System.out.println(sequence + " " + expectedSeq + " " + packetsParsed++);
		if (sequence != expectedSeq) {
			if (lostPackets.contains(sequence)) {
				packetsUnordered++;
				lostPackets.remove(sequence);
			} else if (!allPackets.contains(expectedSeq)) {
				lostPackets.add(expectedSeq);
			}
		}
		allPackets.add(sequence);
		expectedSeq = sequence + 1;

		return new Frame((byte) payloadType, marker, (short) sequence, timestamp, Arrays.copyOfRange(packet, 12, packet.length));
	}

	private enum State {
		INIT, READY, PLAYING
	}
}
