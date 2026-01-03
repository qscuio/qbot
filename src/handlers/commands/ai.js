import * as telegram from '../../telegram/api.js';
import { PROVIDERS } from '../../providers/index.js';
import { callGemini } from '../../providers/gemini.js';
import { callOpenAI } from '../../providers/openai.js';
import { callClaude } from '../../providers/claude.js';
import { callGroq } from '../../providers/groq.js';
import { callNvidia } from '../../providers/nvidia.js';
import { 
  getUserSettings,
  getActiveChat,
  saveMessage,
  getChatMessages,
  getMessageCount,
  getChat,
  renameChat,
  updateChatSummary,
} from '../../db/index.js';

// /ai command
export async function handleAI(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  const prompt = (message.text || '').replace('/ai', '').trim();
  
  if (!prompt) {
    return telegram.sendMessage(chatId, 'Please provide a prompt. Example: /ai What is the moon?');
  }
  
  return processAIRequest(chatId, userId, prompt);
}

// Handle forwarded messages - analyze for truth, politics, market impact
export async function handleForwardedMessage(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  const text = message.text || message.caption || '';
  
  if (!text) {
    return telegram.sendMessage(chatId, '❌ No text content in forwarded message to analyze.');
  }
  
  const settings = await getUserSettings(userId);
  const provider = PROVIDERS[settings.provider];
  
  await telegram.sendChatAction(chatId, 'typing');
  await telegram.sendHtmlMessage(chatId, `🔍 <i>Analyzing forwarded message...</i>`);
  
  const analysisPrompt = `You are a fact-checker and analyst. Analyze the following forwarded message.

For each impact analysis, provide the LOGIC CHAIN showing step-by-step reasoning from event to result.

Provide analysis in this format:
## 📋 Summary
Brief summary of the content.

## ✅ Fact Check
Verify claims. Rate accuracy (Verified/Partially True/Unverified/False/Opinion).
Cite sources or reasoning for your verification.

## 🏛️ Political Impact
Step-by-step logic chain:
1. [Event/Claim] → 
2. [Immediate Effect] → 
3. [Secondary Effect] → 
4. [Political Outcome]

## 📈 Market Impact

### 📈 利好 (Bullish)
Industries: [affected sectors]
Logic chain: [Event] → [Mechanism] → [Positive Effect]
Tickers: [symbols]

### 📉 利空 (Bearish)
Industries: [affected sectors]
Logic chain: [Event] → [Mechanism] → [Negative Effect]
Tickers: [symbols]

## 🔗 Context
Additional context, related events, or background.

Message to analyze:
"""
${text}
"""`;

  try {
    let response;
    switch (settings.provider) {
      case 'gemini':
        response = await callGemini(analysisPrompt, settings.model);
        break;
      case 'openai':
        response = await callOpenAI(analysisPrompt, settings.model);
        break;
      case 'claude':
        response = await callClaude(analysisPrompt, settings.model);
        break;
      case 'groq':
        response = await callGroq(analysisPrompt, settings.model || 'llama-3.3-70b-versatile');
        break;
      case 'nvidia':
      default:
        response = await callNvidia(analysisPrompt, settings.model || 'meta/llama-3.1-70b-instruct');
    }
    
    if (response.content) {
      const responseHtml = `<b>🔍 Analysis (${provider?.name || 'AI'}):</b>\n${telegram.markdownToHtml(response.content)}`;
      await telegram.sendLongHtmlMessage(chatId, responseHtml);
    } else {
      await telegram.sendMessage(chatId, '⚠️ Could not analyze the message.');
    }
  } catch (error) {
    console.error('Forward analysis error:', error);
    await telegram.sendMessage(chatId, `❌ Analysis failed: ${error.message}`);
  }
}

// Process AI request with chat history
export async function processAIRequest(chatId, userId, prompt) {
  const settings = await getUserSettings(userId);
  const provider = PROVIDERS[settings.provider];
  
  if (!provider) {
    return telegram.sendMessage(chatId, 'Invalid provider selected.');
  }
  
  // Get or create active chat
  const activeChat = await getActiveChat(userId);
  
  // Save user message
  await saveMessage(activeChat.id, 'user', prompt);
  
  // Auto-generate title from first message
  const messageCount = await getMessageCount(activeChat.id);
  if (messageCount === 1 && activeChat.title === 'New Chat') {
    const shortTitle = prompt.substring(0, 50) + (prompt.length > 50 ? '...' : '');
    await renameChat(activeChat.id, shortTitle);
  }
  
  // Send typing indicator
  await telegram.sendChatAction(chatId, 'typing');
  
  // Send status message that we'll update
  const statusMsg = await telegram.sendHtmlMessage(chatId, `🤔 <i>Thinking...</i>\n\n<code>${provider.name}: ${settings.model}</code>`);
  const statusMsgId = statusMsg?.result?.message_id;
  
  // Keep sending typing action while processing
  const typingInterval = setInterval(() => {
    telegram.sendChatAction(chatId, 'typing');
  }, 4000);
  
  try {
    // Build context: summary + recent messages
    const recentMessages = await getChatMessages(activeChat.id, 4);
    const history = recentMessages.reverse().map(m => ({
      role: m.role,
      content: m.content,
    }));
    
    // Add summary as system context if available
    const contextPrefix = activeChat.summary 
      ? `[Previous conversation summary: ${activeChat.summary}]\n\n` 
      : '';
    
    // Update status
    if (statusMsgId) {
      await telegram.editMessageText(chatId, statusMsgId, `⏳ <i>Processing...</i>\n\n<code>${provider.name}: ${settings.model}</code>`);
    }
    
    // Create timeout wrapper (60 seconds)
    const timeoutMs = 60000;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    
    let response;
    try {
      switch (settings.provider) {
        case 'gemini':
          response = await callGemini(prompt, settings.model, history, contextPrefix);
          break;
        case 'openai':
          response = await callOpenAI(prompt, settings.model, history, contextPrefix);
          break;
        case 'claude':
          response = await callClaude(prompt, settings.model, history, contextPrefix);
          break;
        case 'groq':
          response = await callGroq(prompt, settings.model, history, contextPrefix);
          break;
        case 'nvidia':
          response = await callNvidia(prompt, settings.model, history, contextPrefix);
          break;
        default:
          clearInterval(typingInterval);
          clearTimeout(timeoutId);
          return telegram.sendMessage(chatId, 'Unknown provider.');
      }
    } finally {
      clearTimeout(timeoutId);
    }
    
    clearInterval(typingInterval);
    
    // Delete status message
    if (statusMsgId) {
      try {
        await telegram.editMessageText(chatId, statusMsgId, `✅ <i>Done!</i>`, 'HTML');
      } catch (e) { /* ignore */ }
    }
    
    // Save assistant response
    if (response.content) {
      await saveMessage(activeChat.id, 'assistant', response.content);
      
      // Update summary every 6 messages
      const newMessageCount = await getMessageCount(activeChat.id);
      if (newMessageCount % 6 === 0) {
        updateConversationSummary(activeChat.id, settings);
      }
    }
    
    // Send thinking process if available
    if (response.thinking) {
      const thinkingHtml = `<b>💭 Thinking:</b>\n<i>${telegram.escapeHtml(response.thinking.substring(0, 1000))}${response.thinking.length > 1000 ? '...' : ''}</i>`;
      await telegram.sendLongHtmlMessage(chatId, thinkingHtml);
    }
    
    // Send the main response
    if (response.content) {
      const responseHtml = `<b>💬 ${provider.name}:</b>\n${telegram.markdownToHtml(response.content)}`;
      await telegram.sendLongHtmlMessage(chatId, responseHtml);
    } else {
      await telegram.sendMessage(chatId, '⚠️ No response from AI. Try a different model or provider.');
    }
    
    // Show quick action buttons
    const buttons = [
      [{ text: '✨ New', callback_data: 'cmd_new' }, { text: '📂 Chats', callback_data: 'cmd_chats' }],
      [{ text: '🔌 Provider', callback_data: 'cmd_providers' }, { text: '📝 Export', callback_data: 'cmd_export' }],
    ];
    await telegram.sendInlineButtons(chatId, '<i>Quick actions:</i>', buttons);
  } catch (error) {
    clearInterval(typingInterval);
    
    // Update status to show error
    if (statusMsgId) {
      try {
        await telegram.editMessageText(chatId, statusMsgId, `❌ <i>Error occurred</i>`, 'HTML');
      } catch (e) { /* ignore */ }
    }
    
    // Detailed error message
    const errorMessage = error.name === 'AbortError' 
      ? '⏱️ Request timed out (60s). The model may be overloaded. Try again or switch to a faster model.'
      : `❌ Error: ${error.message}\n\n<i>Try /models to switch models or /providers to change provider.</i>`;
    
    await telegram.sendHtmlMessage(chatId, errorMessage);
    console.error('AI request error:', error);
  }
}

// Update conversation summary in background
async function updateConversationSummary(chatId, settings) {
  try {
    const chat = await getChat(chatId);
    if (!chat || chat.messages.length < 4) return;
    
    // Create prompt for summarization
    const messagesText = chat.messages
      .map(m => `${m.role}: ${m.content}`)
      .join('\n');
    
    const summaryPrompt = `Summarize this conversation in 2-3 sentences, capturing the key topics and context:\n\n${messagesText}`;
    
    let response;
    switch (settings.provider) {
      case 'gemini':
        response = await callGemini(summaryPrompt, settings.model);
        break;
      case 'openai':
        response = await callOpenAI(summaryPrompt, settings.model);
        break;
      case 'claude':
        response = await callClaude(summaryPrompt, settings.model);
        break;
      case 'nvidia':
        response = await callNvidia(summaryPrompt, settings.model);
        break;
    }
    
    if (response?.content) {
      await updateChatSummary(chatId, response.content);
    }
  } catch (error) {
    console.error('Failed to update summary:', error);
  }
}
