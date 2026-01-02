import express from 'express';
import { config } from './config.js';
import { handleUpdate } from './handlers/index.js';
import { initDatabase } from './db/index.js';
import { initCache } from './cache/index.js';

const app = express();
app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Telegram webhook endpoint
app.post('/webhook', async (req, res) => {
  console.log('📨 Received webhook request');
  
  // Verify webhook secret
  const secretHeader = req.headers['x-telegram-bot-api-secret-token'];
  if (secretHeader !== config.botSecret) {
    console.log('❌ Unauthorized request - secret mismatch');
    console.log('Expected:', config.botSecret);
    console.log('Received:', secretHeader);
    return res.status(403).json({ error: 'Unauthorized' });
  }
  
  // Process update asynchronously
  const update = req.body;
  console.log('✅ Processing update:', JSON.stringify(update).substring(0, 200));
  handleUpdate(update).catch(err => console.error('Error handling update:', err));
  
  res.json({ ok: true });
});

// Start server
async function start() {
  try {
    // Initialize database and cache
    await initDatabase();
    await initCache();
    
    app.listen(config.port, () => {
      console.log(`🤖 Bot server running on port ${config.port}`);
      console.log(`📡 Webhook endpoint: /webhook`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

start();
