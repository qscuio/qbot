import { execSync } from 'child_process';
import { writeFileSync, mkdirSync, existsSync } from 'fs';
import { join } from 'path';
import { config } from '../config.js';
import { getActiveChat, getChat, getUserSettings } from '../db/index.js';
import { callGemini } from '../providers/gemini.js';
import { callOpenAI } from '../providers/openai.js';
import { callClaude } from '../providers/claude.js';
import { callGroq } from '../providers/groq.js';

const NOTES_DIR = '/tmp/qbot-notes';

// Configure SSH to accept GitHub's host key
function setupSSH() {
  try {
    mkdirSync('/root/.ssh', { recursive: true });
    // Add GitHub to known hosts
    execSync('ssh-keyscan github.com >> /root/.ssh/known_hosts 2>/dev/null', { stdio: 'pipe' });
  } catch (e) {
    // Ignore if already exists or not needed
  }
}

export async function exportChatToGit(userId) {
  if (!config.notesRepo) {
    throw new Error('NOTES_REPO not configured');
  }
  
  const activeChat = await getActiveChat(userId);
  const chat = await getChat(activeChat.id);
  
  if (!chat || chat.messages.length === 0) {
    throw new Error('No messages to export');
  }
  
  const settings = await getUserSettings(userId);
  
  // Generate knowledge summary using AI
  const messagesText = chat.messages
    .map(m => `**${m.role === 'user' ? 'User' : 'Assistant'}:** ${m.content}`)
    .join('\n\n');
  
  const summaryPrompt = `Convert this conversation into a well-organized knowledge document in Markdown format. 
Extract key information, insights, and learnings. Use headers, bullet points, and code blocks where appropriate.
Make it useful as a reference document.

Conversation:
${messagesText}`;

  let summary;
  try {
    let response;
    switch (settings.provider) {
      case 'groq':
        response = await callGroq(summaryPrompt, settings.model);
        break;
      case 'gemini':
        response = await callGemini(summaryPrompt, settings.model);
        break;
      case 'openai':
        response = await callOpenAI(summaryPrompt, settings.model);
        break;
      case 'claude':
        response = await callClaude(summaryPrompt, settings.model);
        break;
      default:
        response = await callGroq(summaryPrompt, 'llama-3.3-70b-versatile');
    }
    summary = response.content || messagesText;
  } catch (error) {
    console.error('Failed to generate summary:', error);
    summary = messagesText; // Fallback to raw messages
  }
  
  // Create markdown content
  const date = new Date().toISOString().split('T')[0];
  const safeTitle = chat.title.replace(/[^a-zA-Z0-9\u4e00-\u9fa5]/g, '-').substring(0, 50);
  const filename = `${date}-${safeTitle}.md`;
  
  const markdown = `# ${chat.title}

> Exported on ${new Date().toLocaleString()}

${summary}

---

*Exported from QBot*
`;

  // Setup SSH for git
  setupSSH();

  // Clone/pull repo and push
  try {
    if (!existsSync(NOTES_DIR)) {
      mkdirSync(NOTES_DIR, { recursive: true });
      execSync(`GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=no" git clone ${config.notesRepo} ${NOTES_DIR}`, { stdio: 'pipe' });
    } else {
      execSync(`cd ${NOTES_DIR} && git pull`, { stdio: 'pipe' });
    }
    
    // Create chats directory if not exists
    const chatsDir = join(NOTES_DIR, 'chats');
    if (!existsSync(chatsDir)) {
      mkdirSync(chatsDir, { recursive: true });
    }
    
    // Write file
    const filePath = join(chatsDir, filename);
    writeFileSync(filePath, markdown);
    
    // Configure git user
    execSync(`cd ${NOTES_DIR} && git config user.email "qbot@telegram.bot" && git config user.name "QBot"`, { stdio: 'pipe' });
    
    // Commit and push
    execSync(`cd ${NOTES_DIR} && git add . && git commit -m "Add: ${chat.title}" && git push`, { 
      stdio: 'pipe',
    });
    
    return { filename: `chats/${filename}` };
  } catch (error) {
    console.error('Git operation failed:', error);
    throw new Error(`Git push failed: ${error.message}`);
  }
}

