# Apps Script Whitelist Configuration

## Quick Setup Guide

The Apps Script whitelist can now be configured via Script Properties without redeploying the code.

## Configuration Options

### Script Properties

Add these properties in Apps Script → Project Settings → Script Properties:

| Property | Value | Required | Default |
|----------|-------|----------|---------|
| `WHITELIST_ENABLED` | `true` or `false` | No | `true` |
| `WHITELIST_EMAILS` | JSON array of emails | No | `["niki.cooke@morrisonsplc.co.uk"]` |

## Examples

### Example 1: Enable whitelist with multiple users

```
WHITELIST_ENABLED = true
WHITELIST_EMAILS = ["niki.cooke@morrisonsplc.co.uk", "john.doe@morrisonsplc.co.uk", "jane.smith@morrisonsplc.co.uk"]
```

### Example 2: Disable whitelist (allow everyone)

```
WHITELIST_ENABLED = false
```

**⚠️ Warning:** Disabling the whitelist allows anyone with the deployment URL to trigger workflows. Only do this if you trust all users in your Google Chat space.

### Example 3: Use default (no configuration needed)

If you don't set any Script Properties, the system uses:
- `WHITELIST_ENABLED = true`
- `WHITELIST_EMAILS = ["niki.cooke@morrisonsplc.co.uk"]`

## How to Update

1. Open your Apps Script project at [script.google.com](https://script.google.com)
2. Click **Project Settings** (gear icon)
3. Scroll to **Script Properties**
4. Click **Edit script properties**
5. Add or modify the properties
6. Click **Save**

**Changes take effect immediately** - no need to redeploy!

## Testing

After configuring, test by clicking a Quick Actions button:

- ✅ **Authorized user**: Workflow triggers successfully
- 🚫 **Unauthorized user**: Sees "Access Denied" message
- 📝 **All attempts**: Logged in Apps Script execution logs

## Troubleshooting

### "Access Denied" for authorized user

1. Check the email address in `WHITELIST_EMAILS` matches exactly (case-sensitive)
2. Verify `WHITELIST_ENABLED` is set to `true` (or not set)
3. Check Apps Script execution logs for the actual email being used

### Whitelist not working (everyone can trigger)

1. Verify `WHITELIST_ENABLED` is set to `true` (not `"true"` in quotes)
2. Check that `WHITELIST_EMAILS` is valid JSON format
3. Review Apps Script execution logs for parsing errors

### JSON format errors

The `WHITELIST_EMAILS` value must be valid JSON:

✅ **Correct:**
```json
["email1@example.com", "email2@example.com"]
```

❌ **Incorrect:**
```
email1@example.com, email2@example.com
```

## Security Notes

- The deployment URL should remain secret
- Only share it with trusted users
- All trigger attempts are logged with user attribution
- Consider keeping the whitelist enabled for production use
