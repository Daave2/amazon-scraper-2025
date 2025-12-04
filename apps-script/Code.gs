/**
 * ==============================================================================
 * GOOGLE APPS SCRIPT - GITHUB WORKFLOW TRIGGER
 * ==============================================================================
 * 
 * This web app receives Google Chat card button clicks and triggers GitHub
 * workflows via the repository_dispatch API.
 * 
 * SETUP:
 * 1. Create a GitHub Fine-Grained Personal Access Token with:
 *    - Repository: Daave2/amazon-scraper
 *    - Permissions: Actions (Read and write)
 * 2. Store the PAT in Script Properties:
 *    - Key: GH_PAT
 *    - Value: your_github_pat_here
 * 3. Deploy as Web App:
 *    - Execute as: Me
 *    - Who has access: Anyone with the link
 * 4. Copy deployment URL to config.json as "apps_script_webhook_url"
 * 
 * ==============================================================================
 */

// Configuration
const GITHUB_OWNER = 'Daave2';
const GITHUB_REPO = 'amazon-scraper-2025';
const GITHUB_API_BASE = 'https://api.github.com';
const COOLDOWN_MINUTES = 30; // Prevent duplicate runs within this window
const COOLDOWN_MS = COOLDOWN_MINUTES * 60 * 1000;

/**
 * Main entry point for Google Chat webhook
 * Handles GET requests from "openLink" buttons
 */
function doGet(e) {
  try {
    const params = e.parameter;
    const eventType = params.event_type;
    
    if (!eventType) {
      return HtmlService.createHtmlOutput("❌ Error: Missing event_type parameter.");
    }
    
    const dateMode = params.date_mode || 'today';
    const topN = params.top_n || '10';
    const sender = Session.getActiveUser().getEmail(); // Securely get user email
    
    // Whitelist Check (Configurable via Script Properties)
    const props = PropertiesService.getScriptProperties();
    const whitelistEnabled = props.getProperty('WHITELIST_ENABLED');
    
    // If whitelist is explicitly disabled, skip the check
    if (whitelistEnabled !== null && whitelistEnabled.toLowerCase() === 'false') {
      Logger.log(`Whitelist disabled - allowing access for: ${sender}`);
    } else {
      // Whitelist is enabled (default behavior)
      // Try to get whitelist from Script Properties first
      let whitelist = [];
      const whitelistProperty = props.getProperty('WHITELIST_EMAILS');
      
      if (whitelistProperty) {
        try {
          whitelist = JSON.parse(whitelistProperty);
          Logger.log(`Using whitelist from Script Properties: ${whitelist.join(', ')}`);
        } catch (e) {
          Logger.log(`Error parsing WHITELIST_EMAILS: ${e}. Using default.`);
          whitelist = ['niki.cooke@morrisonsplc.co.uk'];
        }
      } else {
        // Fallback to hardcoded default
        whitelist = ['niki.cooke@morrisonsplc.co.uk'];
        Logger.log(`Using default whitelist: ${whitelist.join(', ')}`);
      }
      
      if (!sender || !whitelist.includes(sender)) {
        Logger.log(`Access Denied: ${sender} is not in whitelist.`);
        return HtmlService.createHtmlOutput(`
          <div style="font-family: sans-serif; text-align: center; padding-top: 50px; color: red;">
            <h1>🚫 Access Denied</h1>
            <p>User <b>${sender || 'Unknown'}</b> is not authorized to trigger workflows.</p>
            <p>Please contact the administrator to request access.</p>
          </div>
        `);
      }
    }
    
    Logger.log(`GET Trigger: ${eventType}, date_mode: ${dateMode}, requested by: ${sender}`);
    
    // Build client payload
    const payload = {
      date_mode: dateMode,
      requested_by: sender,
      source: 'google-chat-link',
      top_n: eventType === 'run-inf-analysis' ? parseInt(topN) : undefined
    };

    // Trigger GitHub workflow
    const result = triggerGitHubWorkflow(eventType, payload);

    if (result.success) {
      return HtmlService.createHtmlOutput(`
        <div style="font-family: sans-serif; text-align: center; padding-top: 50px;">
          <h1>✅ Workflow Triggered!</h1>
          <p><b>${getWorkflowDisplayName(eventType)}</b> is now running.</p>
          <p>Requested by: ${sender}</p>
          <p>You can close this tab now.</p>
          <script>setTimeout(function(){ window.close(); }, 3000);</script>
        </div>
      `);
    }

    if (result.cooldownActive) {
      const { remainingMinutes, lastRequestedBy } = result.cooldownInfo;
      return HtmlService.createHtmlOutput(`
        <div style="font-family: sans-serif; text-align: center; padding-top: 50px; color: #c97a00;">
          <h1>⚠️ Workflow Already Requested</h1>
          <p><b>${getWorkflowDisplayName(eventType)}</b> was triggered recently.</p>
          <p>Requested by: ${lastRequestedBy || 'someone else'}</p>
          <p>Please wait ~${remainingMinutes} minute(s) before trying again.</p>
        </div>
      `);
    }

    return HtmlService.createHtmlOutput(`
      <div style="font-family: sans-serif; text-align: center; padding-top: 50px; color: red;">
        <h1>❌ Trigger Failed</h1>
        <p>Error: ${result.error}</p>
      </div>
    `);
    
  } catch (error) {
    return HtmlService.createHtmlOutput("❌ Error: " + error.message);
  }
}

function doPost(e) {
  // Keep doPost for health checks or future Chat App integration
  return ContentService.createTextOutput("POST requests active");
}

/**
 * Handle text-based commands (optional)
 */
function handleTextCommand(text, sender, spaceName) {
  const lowerText = text.toLowerCase().trim();
  
  // Match commands like "run inf", "run performance", etc.
  if (lowerText.match(/run.*inf/)) {
    return handleCardClick({
      parameters: [
        { key: 'event_type', value: 'run-inf-analysis' },
        { key: 'date_mode', value: 'today' }
      ]
    }, sender, spaceName);
  }
  
  if (lowerText.match(/run.*performance/)) {
    return handleCardClick({
      parameters: [
        { key: 'event_type', value: 'run-performance-check' },
        { key: 'date_mode', value: 'today' }
      ]
    }, sender, spaceName);
  }
  
  if (lowerText.match(/run.*full|run.*scrape/)) {
    return handleCardClick({
      parameters: [
        { key: 'event_type', value: 'run-full-scrape' },
        { key: 'date_mode', value: 'today' }
      ]
    }, sender, spaceName);
  }
  
  // Help command
  if (lowerText.includes('help')) {
    return buildJsonResponse({
      text: '🤖 **Available Commands:**\n' +
            '• "run inf" - Run INF analysis\n' +
            '• "run performance" - Run performance check\n' +
            '• "run full scrape" - Run full scraper\n' +
            '\nOr use the Quick Actions buttons on report cards!'
    });
  }
  
  // Ignore other messages
  return ContentService.createTextOutput('OK');
}

/**
 * Trigger GitHub workflow via repository_dispatch API
 */
function triggerGitHubWorkflow(eventType, clientPayload) {
  let lock;
  try {
    lock = LockService.getScriptLock();
    lock.waitLock(5000);

    const cooldownStatus = getCooldownStatus(eventType);
    if (cooldownStatus.active) {
      return {
        success: false,
        cooldownActive: true,
        cooldownInfo: {
          remainingMinutes: cooldownStatus.remainingMinutes,
          lastRequestedBy: cooldownStatus.lastRequestedBy
        },
        error: cooldownStatus.message
      };
    }

    // Get GitHub PAT from Script Properties
    const token = PropertiesService.getScriptProperties().getProperty('GH_PAT');
    
    if (!token) {
      throw new Error('GitHub PAT not configured in Script Properties');
    }

    // --- NEW: Send Acknowledgement to Google Chat ---
    // This lets everyone know who triggered what
    try {
      const webhookUrl = PropertiesService.getScriptProperties().getProperty('CHAT_WEBHOOK_URL');
      if (webhookUrl) {
        const workflowName = getWorkflowDisplayName(eventType);
        const requestor = clientPayload.requested_by || 'Unknown User';
        
        const ackPayload = {
          cardsV2: [{
            cardId: `ack-${Date.now()}`,
            card: {
              header: {
                title: '⏳ Workflow Started',
                subtitle: `${workflowName}`,
                imageUrl: 'https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png',
                imageType: 'CIRCLE'
              },
              sections: [{
                widgets: [
                  {
                    textParagraph: {
                      text: `<b>${requestor}</b> has requested a report.<br>Running now, please wait...`
                    }
                  }
                ]
              }]
            }
          }]
        };
        
        UrlFetchApp.fetch(webhookUrl, {
          method: 'post',
          contentType: 'application/json',
          payload: JSON.stringify(ackPayload),
          muteHttpExceptions: true
        });
      }
    } catch (e) {
      Logger.log('Failed to send ack to chat: ' + e);
      // Don't fail the whole trigger just because ack failed
    }
    // ------------------------------------------------

    const url = `${GITHUB_API_BASE}/repos/${GITHUB_OWNER}/${GITHUB_REPO}/dispatches`;
    
    const payload = JSON.stringify({
      event_type: eventType,
      client_payload: clientPayload
    });
    
    const options = {
      method: 'post',
      contentType: 'application/json',
      headers: {
        'Authorization': 'Bearer ' + token,
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28'
      },
      payload: payload,
      muteHttpExceptions: true
    };
    
    Logger.log('Triggering workflow: ' + url);
    Logger.log('Payload: ' + payload);
    
    const response = UrlFetchApp.fetch(url, options);
    const responseCode = response.getResponseCode();
    
    Logger.log('GitHub API response code: ' + responseCode);
    
    if (responseCode === 204) {
      // Success - repository_dispatch returns 204 No Content
      recordTrigger(eventType, clientPayload.requested_by);
      return { success: true };
    } else {
      const errorText = response.getContentText();
      Logger.log('GitHub API error: ' + errorText);
      return { success: false, error: `API returned ${responseCode}: ${errorText}` };
    }

  } catch (error) {
    Logger.log('Error triggering workflow: ' + error);
    return { success: false, error: error.message };
  } finally {
    if (lock) {
      try {
        lock.releaseLock();
      } catch (releaseError) {
        Logger.log('Error releasing lock: ' + releaseError);
      }
    }
  }
}

/**
 * Determine if the workflow is on cooldown
 */
function getCooldownStatus(eventType) {
  const props = PropertiesService.getScriptProperties();
  const key = `LAST_TRIGGER_${eventType}`;
  const raw = props.getProperty(key);

  if (!raw) {
    return { active: false };
  }

  try {
    const parsed = JSON.parse(raw);
    const lastTimestamp = parsed.timestamp;
    const lastRequestedBy = parsed.requested_by;
    const now = Date.now();
    const elapsed = now - lastTimestamp;

    if (elapsed < COOLDOWN_MS) {
      const remainingMs = COOLDOWN_MS - elapsed;
      const remainingMinutes = Math.ceil(remainingMs / (60 * 1000));
      return {
        active: true,
        remainingMinutes,
        lastRequestedBy,
        message: `Workflow recently triggered by ${lastRequestedBy || 'someone else'}. Wait ${remainingMinutes} minute(s).`
      };
    }
  } catch (error) {
    Logger.log('Error parsing cooldown state: ' + error);
  }

  return { active: false };
}

/**
 * Record the last trigger time for cooldown enforcement
 */
function recordTrigger(eventType, requestedBy) {
  const props = PropertiesService.getScriptProperties();
  const key = `LAST_TRIGGER_${eventType}`;
  const payload = {
    timestamp: Date.now(),
    requested_by: requestedBy
  };

  props.setProperty(key, JSON.stringify(payload));
}

/**
 * Helper: Extract parameter value by key
 */
function getParameter(params, key) {
  const param = params.find(p => p.key === key);
  return param ? param.value : null;
}

/**
 * Build success response card
 */
function buildSuccessResponse(eventType, dateMode, requestedBy) {
  const workflowName = getWorkflowDisplayName(eventType);
  const actionsUrl = `https://github.com/${GITHUB_OWNER}/${GITHUB_REPO}/actions`;
  
  return buildJsonResponse({
    cardsV2: [{
      cardId: 'trigger-success-' + new Date().getTime(),
      card: {
        header: {
          title: '✅ Workflow Triggered',
          subtitle: 'GitHub Actions is running your request',
          imageUrl: 'https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png',
          imageType: 'CIRCLE'
        },
        sections: [{
          widgets: [
            {
              textParagraph: {
                text: `<b>Workflow:</b> ${workflowName}<br>` +
                      `<b>Date Mode:</b> ${dateMode}<br>` +
                      `<b>Requested by:</b> ${requestedBy}<br><br>` +
                      `⏳ The workflow is now running. Results will be posted to this chat when complete.`
              }
            },
            {
              buttonList: {
                buttons: [{
                  text: '🔗 View on GitHub',
                  onClick: {
                    openLink: {
                      url: actionsUrl
                    }
                  }
                }]
              }
            }
          ]
        }]
      }
    }]
  });
}

/**
 * Build error response card
 */
function buildErrorResponse(eventType, error) {
  return buildJsonResponse({
    cardsV2: [{
      cardId: 'trigger-error-' + new Date().getTime(),
      card: {
        header: {
          title: '❌ Workflow Trigger Failed',
          subtitle: 'There was a problem starting the workflow',
          imageType: 'CIRCLE'
        },
        sections: [{
          widgets: [{
            textParagraph: {
              text: `<b>Workflow:</b> ${getWorkflowDisplayName(eventType)}<br>` +
                    `<b>Error:</b> ${error}<br><br>` +
                    `Please check the GitHub Actions configuration and try again.`
            }
          }]
        }]
      }
    }]
  });
}

/**
 * Get user-friendly workflow name
 */
function getWorkflowDisplayName(eventType) {
  const names = {
    'run-inf-analysis': 'INF Analysis',
    'run-performance-check': 'Performance Highlights',
    'run-full-scrape': 'Full Scraper Run'
  };
  return names[eventType] || eventType;
}

/**
 * Build JSON response for Google Chat
 */
function buildJsonResponse(json) {
  return ContentService
    .createTextOutput(JSON.stringify(json))
    .setMimeType(ContentService.MimeType.JSON);
}

/**
 * Test function (only works when running in Apps Script editor)
 */
function testTrigger() {
  const result = triggerGitHubWorkflow('run-inf-analysis', {
    date_mode: 'today',
    requested_by: 'Test User',
    source: 'apps-script-test'
  });
  
  Logger.log('Test result: ' + JSON.stringify(result));
}
