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
const GITHUB_REPO = 'amazon-scraper';
const GITHUB_API_BASE = 'https://api.github.com';

/**
 * Main entry point for Google Chat webhook
 * Handles both card interactions and simple message commands
 */
function doPost(e) {
  try {
    // Parse the request
    const body = JSON.parse(e.postData.contents || '{}');
    
    Logger.log('Received request: ' + JSON.stringify(body));
    
    // Extract user info
    const sender = body.message?.sender?.displayName || body.user?.displayName || 'Unknown';
    const spaceName = body.space?.displayName || 'Unknown Space';
    
    // Check for card click action
    if (body.type === 'CARD_CLICKED' && body.action) {
      return handleCardClick(body.action, sender, spaceName);
    }
    
    // Check for text command (e.g., "run inf analysis")
    if (body.message?.text) {
      return handleTextCommand(body.message.text, sender, spaceName);
    }
    
    // Unknown request type
    return buildJsonResponse({
      text: '❓ Sorry, I didn\'t understand that request.'
    });
    
  } catch (error) {
    Logger.log('Error in doPost: ' + error);
    return buildJsonResponse({
      text: '❌ Error: ' + error.message
    });
  }
}

/**
 * Handle card button click events
 */
function handleCardClick(action, sender, spaceName) {
  const params = action.parameters || [];
  
  // Extract parameters
  const eventType = getParameter(params, 'event_type');
  const dateMode = getParameter(params, 'date_mode') || 'today';
  const topN = getParameter(params, 'top_n') || '5';
  
  if (!eventType) {
    return buildJsonResponse({
      text: '❌ Missing event_type parameter'
    });
  }
  
  Logger.log(`Button clicked: ${eventType}, date_mode: ${dateMode}, requested by: ${sender}`);
  
  // Build client payload
  const payload = {
    date_mode: dateMode,
    requested_by: sender,
    source: 'google-chat',
    space: spaceName
  };
  
  // Add top_n for INF analysis
  if (eventType === 'run-inf-analysis' && topN) {
    payload.top_n = parseInt(topN);
  }
  
  // Trigger GitHub workflow
  const result = triggerGitHubWorkflow(eventType, payload);
  
  if (result.success) {
    return buildSuccessResponse(eventType, dateMode, sender);
  } else {
    return buildErrorResponse(eventType, result.error);
  }
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
  try {
    // Get GitHub PAT from Script Properties
    const token = PropertiesService.getScriptProperties().getProperty('GH_PAT');
    
    if (!token) {
      throw new Error('GitHub PAT not configured in Script Properties');
    }
    
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
      return { success: true };
    } else {
      const errorText = response.getContentText();
      Logger.log('GitHub API error: ' + errorText);
      return { success: false, error: `API returned ${responseCode}: ${errorText}` };
    }
    
  } catch (error) {
    Logger.log('Error triggering workflow: ' + error);
    return { success: false, error: error.message };
  }
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
