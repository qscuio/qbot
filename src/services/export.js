import { execSync } from 'child_process';
import { writeFileSync, mkdirSync, existsSync, readFileSync } from 'fs';
import { join } from 'path';
import { config } from '../config.js';
import { getActiveChat, getChat, getUserSettings } from '../db/index.js';
import { callGemini } from '../providers/gemini.js';
import { callOpenAI } from '../providers/openai.js';
import { callClaude } from '../providers/claude.js';
import { callGroq } from '../providers/groq.js';

const NOTES_DIR = '/tmp/qbot-notes';
const SSH_DIR = '/tmp/.ssh';
const MOUNTED_SSH_KEY = '/app/.qbot_ssh/deploy_key';

// Configure SSH for git operations
function setupSSH() {
  if (!existsSync(MOUNTED_SSH_KEY)) {
    throw new Error('SSH key not found. Please set VPS_SSH_KEY secret.');
  }
  
  mkdirSync(SSH_DIR, { recursive: true });
  
  const keyContent = readFileSync(MOUNTED_SSH_KEY, 'utf8');
  const destKey = `${SSH_DIR}/id_rsa`;
  writeFileSync(destKey, keyContent, { mode: 0o600 });
  
  const sshConfig = `Host github.com
  HostName github.com
  User git
  IdentityFile ${destKey}
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
`;
  writeFileSync(`${SSH_DIR}/config`, sshConfig, { mode: 0o600 });
}

function getGitSSHCommand() {
  return `GIT_SSH_COMMAND="ssh -F ${SSH_DIR}/config"`;
}

// Generate short filename: MMDD-first-few-words.md
function generateFilename(title, date) {
  const mmdd = `${String(date.getMonth() + 1).padStart(2, '0')}${String(date.getDate()).padStart(2, '0')}`;
  const words = title.split(/\s+/).slice(0, 3).join('-');
  const safeWords = words.replace(/[^a-zA-Z0-9\u4e00-\u9fa5-]/g, '').substring(0, 30);
  return `${mmdd}-${safeWords || 'chat'}.md`;
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
  const date = new Date();
  const filename = generateFilename(chat.title, date);
  
  // Build raw conversation text
  const rawContent = chat.messages
    .map(m => `**${m.role === 'user' ? 'User' : 'Assistant'}:**\n${m.content}`)
    .join('\n\n---\n\n');
  
  // Create raw markdown file
  const rawMarkdown = `# ${chat.title}

> Exported: ${date.toLocaleString()} | ${chat.messages.length} messages

${rawContent}

---
*Exported from QBot*
`;

  // Generate AI summary
  let summaryMarkdown = '';
  try {
    const summaryPrompt = `Analyze this conversation and create a structured knowledge summary.
Extract key points, insights, code examples, and actionable information.
Use headers (##), bullet points, and code blocks.
Be concise but comprehensive.

Conversation:
${rawContent.substring(0, 15000)}`;

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
    
    const summary = response.content || 'Summary generation failed.';
    
    summaryMarkdown = `# ${chat.title} - Notes

> Summary of ${chat.messages.length} messages | ${date.toLocaleString()}

${summary}

---
*AI-generated summary from QBot*
`;
  } catch (error) {
    console.error('Failed to generate summary:', error);
    summaryMarkdown = `# ${chat.title} - Notes

> Summary generation failed

See raw file for full conversation.

---
*Exported from QBot*
`;
  }

  // Setup SSH and clone/pull repo
  setupSSH();
  const gitSSH = getGitSSHCommand();

  try {
    if (!existsSync(NOTES_DIR)) {
      mkdirSync(NOTES_DIR, { recursive: true });
      execSync(`${gitSSH} git clone ${config.notesRepo} ${NOTES_DIR}`, { stdio: 'pipe' });
    } else {
      execSync(`cd ${NOTES_DIR} && ${gitSSH} git pull`, { stdio: 'pipe' });
    }
    
    // Create directories
    const rawDir = join(NOTES_DIR, 'raw');
    const notesDir = join(NOTES_DIR, 'notes');
    mkdirSync(rawDir, { recursive: true });
    mkdirSync(notesDir, { recursive: true });
    
    // Write files
    writeFileSync(join(rawDir, filename), rawMarkdown);
    writeFileSync(join(notesDir, filename), summaryMarkdown);
    
    // Git commit and push
    execSync(`cd ${NOTES_DIR} && git config user.email "qbot@telegram.bot" && git config user.name "QBot"`, { stdio: 'pipe' });
    execSync(`cd ${NOTES_DIR} && git add . && git commit -m "Export: ${chat.title}" && ${gitSSH} git push`, { stdio: 'pipe' });
    
    // Generate GitHub URLs
    let rawUrl = null;
    let notesUrl = null;
    if (config.notesRepo) {
      let repoUrl = config.notesRepo
        .replace('git@github.com:', 'https://github.com/')
        .replace('.git', '');
      if (repoUrl.endsWith('.git')) repoUrl = repoUrl.slice(0, -4);
      
      rawUrl = `${repoUrl}/blob/main/raw/${filename}`;
      notesUrl = `${repoUrl}/blob/main/notes/${filename}`;
    }
    
    return { 
      rawFile: `raw/${filename}`,
      notesFile: `notes/${filename}`,
      rawUrl,
      notesUrl
    };
  } catch (error) {
    console.error('Git operation failed:', error);
    throw new Error(`Git push failed: ${error.message}`);
  }
}
