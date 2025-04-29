import mqtt from 'mqtt';
import dotenv from 'dotenv';
import { WebSocketServer } from 'ws';
import http from 'http';

// Load environment variables
dotenv.config();

// Create HTTP server
const server = http.createServer();

// Create WebSocket server
const wss = new WebSocketServer({ server });

// MQTT topics (matching the publisher)
const MQTT_TOPICS = {
  TEMPERATURE: 'farm/sensors/temperature',
  COOP_A: 'farm/coops/a/temperature',
  COOP_B: 'farm/coops/b/temperature',
  COOP_C: 'farm/coops/c/temperature',
  VENTILATION: 'farm/ventilation/temperature',
  PROCESSING: 'farm/processing/temperature'
};

// MQTT client setup with credentials
const client = mqtt.connect(process.env.BROKER_URL, {
  username: process.env.USER,
  password: process.env.PASSWORD
});

// Store connected WebSocket clients
const clients = new Set();

// Handle WebSocket connections
wss.on('connection', (ws) => {
  console.log('New client connected');
  clients.add(ws);

  ws.on('error', console.error);

  ws.on('close', () => {
    console.log('Client disconnected');
    clients.delete(ws);
  });
});

// Handle MQTT connection events
client.on('connect', () => {
  console.log('Connected to MQTT broker:', process.env.BROKER_URL);
  
  // Subscribe to all temperature topics
  const topics = Object.values(MQTT_TOPICS);
  client.subscribe(topics, { qos: 1 }, (err) => {
    if (err) {
      console.error('Subscription error:', err);
    } else {
      console.log('Successfully subscribed to topics:', topics);
    }
  });
});

// Handle incoming MQTT messages
client.on('message', (topic, message) => {
  try {
    const data = JSON.parse(message.toString());
    console.log(`Received message on topic ${topic}:`, data);
    
    // Broadcast to all connected WebSocket clients
    const wsMessage = JSON.stringify({
      topic,
      data
    });

    clients.forEach((client) => {
      if (client.readyState === 1) { // OPEN
        client.send(wsMessage);
      }
    });
  } catch (error) {
    console.error('Error parsing message:', error);
  }
});

client.on('error', (err) => {
  console.error('MQTT connection error:', err);
});

// Start the server
const PORT = 3001;
server.listen(PORT, () => {
  console.log(`WebSocket server running on port ${PORT}`);
});

export default client;


