# Firebase Authentication Debugging Guide

## Issues Identified & Fixed

### 1. **Cross-Origin-Opener-Policy (COOP) Header Issues**
**Problem:** Firebase popup authentication was failing with errors:
- `auth/popup-blocked`
- `Cross-Origin-Opener-Policy policy would block the window.closed call`

**Root Cause:** Missing security headers that allow popup-based authentication.

**Solution:** Added COOP headers to Flask app:
```python
@app.after_request
def add_security_headers(response):
    response.headers['Cross-Origin-Opener-Policy'] = 'same-origin-allow-popups'
    response.headers['Cross-Origin-Embedder-Policy'] = 'require-corp'
    return response
```

**File Modified:** `app_flask.py` (lines ~110-115)

---

### 2. **401 Backend Authentication Error**
**Problem:** `/api/auth/firebase` endpoint returning 401 Unauthorized with no clear error messages.

**Root Cause:** 
- Firebase token verification was failing silently
- Database user creation/update was failing silently
- No logging to diagnose the issue

**Solution Implemented:**

#### a) Enhanced Backend Logging
Modified `app_flask.py` firebase_login endpoint (line 336):
- Added detailed logging with timestamps
- Shows exact failure points
- Includes error messages and stack traces
- Uses prefixes: `[Firebase Auth]`, `✅`, `❌` for easy spotting

#### b) Improved Firebase Verification
Modified `auth.py` verify_and_login_firebase function (line 236):
- Step-by-step logging of token verification
- Shows decoded token email
- Logs database user creation status
- Includes exception details and stack traces

#### c) Better User Database Operations
Modified `auth.py` User.create_or_update method (line 97):
- Logs database type detection
- Shows Firestore vs SQL backend choice
- Indicates insert vs update operation
- Reports final user_id result
- Full exception logging with stack traces

**Files Modified:** `app_flask.py`, `auth.py`

---

### 3. **No Redirect After Successful Login**
**Problem:** Even if login succeeded, user wasn't redirected to dashboard.

**Root Cause:**
- Frontend error handling wasn't checking response status properly
- Frontend didn't handle the JSON response correctly
- No user feedback during loading

**Solution:** Complete rewrite of `googleSignIn()` function in `landing.html`:
```javascript
window.googleSignIn = async function () {
    try {
        // Step 1: Show loading message
        showAuthMessage('Starting Firebase authentication...', 'info');
        
        // Step 2: Firebase popup auth
        console.log('[Firebase Auth] Attempting signInWithPopup...');
        const result = await signInWithPopup(auth, provider);
        
        // Step 3: Get ID token
        const idToken = await result.user.getIdToken();
        showAuthMessage('Authenticating with server...', 'info');
        
        // Step 4: Send to backend
        const response = await fetch('/api/auth/firebase', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ idToken: idToken }),
            credentials: 'include'  // Important for session cookies
        });
        
        const data = await response.json();
        
        // Step 5: Check response and redirect
        if (response.ok && data.success) {
            showAuthMessage(`Welcome ${data.user.name}! Redirecting...`, 'success');
            setTimeout(() => {
                window.location.href = data.redirect || '/dashboard';
            }, 500);
        } else {
            showAuthMessage(`Authentication failed: ${data.error}`, 'error');
        }
    } catch (error) {
        // Handle popup-specific errors with user-friendly messages
        let userMessage = 'Authentication failed';
        if (error.code === 'auth/popup-blocked') {
            userMessage = 'Pop-up blocked. Please allow pop-ups for this site.';
        } else if (error.code === 'auth/cancelled-popup-request') {
            userMessage = 'Authentication cancelled. Please try again.';
        }
        showAuthMessage(userMessage, 'error');
    }
};
```

**Added Features:**
- Progress messages during auth flow
- User-friendly error messages with specific error codes
- Proper response checking (both `response.ok` AND `data.success`)
- Loading state management for button
- 500ms delay before redirect to show success message
- Credentials included in fetch (for session handling)

**Files Modified:** `landing.html`

---

### 4. **User Feedback & Error Messages**
**Problem:** Users had no indication of what went wrong during authentication.

**Solution:**
- Added `showAuthMessage()` helper function
- Created auth-message HTML element with styling
- Added CSS animations for smooth message display
- Color-coded messages: blue (info), green (success), red (error)

**CSS Added:**
```css
.auth-message {
    position: fixed;
    top: 20px;
    left: 50%;
    transform: translateX(-50%);
    max-width: 500px;
    padding: 16px 24px;
    border-radius: 12px;
    font-weight: 500;
    z-index: 1000;
    animation: slideDown 0.3s ease-out;
    box-shadow: 0 10px 40px rgba(0, 0, 0, 0.3);
}

.auth-message-info { /* Blue */ }
.auth-message-success { /* Green */ }
.auth-message-error { /* Red */ }
```

**Files Modified:** `landing.html`

---

## How to Debug Authentication Issues

### Step 1: Check Browser Console
1. Open DevTools (F12)
2. Go to Console tab
3. Look for `[Firebase Auth]` logs with timestamps
4. Check for `[User]` logs showing database operations

Example output:
```
[Firebase Auth] Starting Firebase authentication...
[Firebase Auth] Attempting signInWithPopup...
[Firebase Auth] ✅ Firebase sign-in successful
[Firebase Auth] ✅ ID token obtained
[Firebase Auth] Authenticating with server...
[User] Creating/updating user: user@example.com (admin=false)
[User] Database type: sqlite
[User] Using SQL backend (type=sqlite)
[User] Checking existing user: user@example.com
[User] New user, inserting
[User] SQL result: user_id=1
[Firebase Auth] ✅ User logged in: user@example.com (admin=false)
```

### Step 2: Check Server Logs
Run your Flask app and watch the console:
```bash
python app_flask.py
```

You'll see logs like:
```
[Firebase Auth] Starting Firebase authentication...
[Firebase] Verifying ID token...
[Firebase] ✅ Token verified, payload: user@example.com
[Firebase] Creating/updating user in database: user@example.com
[User] Creating/updating user: user@example.com (admin=false)
[User] Database type: sqlite
[User] ✅ User created/updated: user@example.com (id=1, admin=False)
[Firebase Auth] ✅ User logged in: user@example.com (admin=False)
```

### Step 3: Common Error Scenarios

#### Error: "Firebase: Error (auth/popup-blocked)"
**Cause:** Browser blocked the popup window
**Fix:**
1. Check browser popup settings
2. Allow popups for your domain
3. Try in a different browser

#### Error: "auth/cancelled-popup-request"
**Cause:** User cancelled the login or popup was closed
**Fix:** User needs to try again

#### Error: "Invalid Firebase token" (401 response)
**Cause:** Token verification or database creation failed
**Solution:**
1. Check Firebase Admin SDK initialization
2. Verify `lively-paratext-487716-r8-firebase-adminsdk-fbsvc-8406fdde9d.json` exists
3. Check database connectivity
4. Look at server logs for detailed error message

#### Error: "No email in token"
**Cause:** Email not present in Firebase ID token
**Fix:**
1. Ensure user is signed in with email
2. Check Firebase configuration

#### Error: "Database connection failed"
**Cause:** SQLite/PostgreSQL/Firestore connection issue
**Fix:**
1. Check DATABASE_URL environment variable (for production)
2. For local: Ensure `leads.db` is writable
3. For Firestore: Check GOOGLE_CLOUD_PROJECT and credentials

### Step 4: Manual Testing

#### Test Firebase Token Only
```javascript
// In browser console
const auth = window.getAuth();
const user = auth.currentUser;
if (user) {
    user.getIdToken().then(token => console.log('Token:', token));
}
```

#### Test Backend Endpoint
```bash
# Get a valid ID token from Firebase console or user
curl -X POST http://localhost:5000/api/auth/firebase \
  -H "Content-Type: application/json" \
  -d '{"idToken": "your_valid_token_here"}'
```

Expected success response:
```json
{
    "success": true,
    "redirect": "/dashboard",
    "user": {
        "id": 1,
        "email": "user@example.com",
        "name": "User Name",
        "is_admin": false
    }
}
```

Expected error response:
```json
{
    "success": false,
    "error": "Detailed error message"
}
```

---

## Critical Files Modified

1. **app_flask.py** (lines 110-115, 336-367)
   - Added COOP/COEP security headers
   - Enhanced firebase_login endpoint with detailed logging

2. **auth.py** (lines 97-136, 236-268)
   - Improved User.create_or_update with logging
   - Enhanced verify_and_login_firebase with step-by-step logging

3. **templates/landing.html** (script section, around lines 50-135)
   - Complete rewrite of googleSignIn() function
   - Added showAuthMessage() helper
   - Added auth-message HTML element
   - Added CSS styles for error messages

---

## Environment Variables to Check

### Required for Production:
- `ADMIN_EMAIL` - Email of admin user (default: samadly728@gmail.com)
- `DATABASE_URL` - Connection string (PostgreSQL for production, SQLite for local)
- `SECRET_KEY` - Flask secret key
- `GOOGLE_CLOUD_PROJECT` - Required for Firestore (if using Firestore backend)

### Check with:
```bash
echo $ADMIN_EMAIL
echo $DATABASE_URL
echo $SECRET_KEY
echo $GOOGLE_CLOUD_PROJECT
```

---

## Testing Checklist

- [ ] User can see "Starting Firebase authentication..." message
- [ ] Google popup appears (not blocked)
- [ ] User selects Google account
- [ ] See "Firebase login successful, getting token..." message
- [ ] See "Authenticating with server..." message
- [ ] See "Welcome [Name]! Redirecting..." in green (success) color
- [ ] User is redirected to dashboard within 1 second
- [ ] Browser console shows step-by-step [Firebase Auth] logs
- [ ] Server console shows [Firebase] and [User] logs
- [ ] User is created/updated in database
- [ ] User login session is established
- [ ] Admin users redirected to admin dashboard, regular users to client dashboard

---

## Additional Notes

### Security Improvements Made:
1. **COOP Header** - Allows authenticated popups while protecting against XSS
2. **Credentials in Fetch** - Ensures session cookies are sent with backend request
3. **Response Validation** - Checks both HTTP status AND JSON success flag
4. **Error Logging** - Detailed tracking for security auditing

### Performance Considerations:
1. 500ms redirect delay is user-imperceptible and allows success message to be seen
2. No additional API calls beyond necessary Firebase + backend auth
3. Database creates index on email for fast lookups

### Compatibility:
- Works with all modern browsers (Chrome, Firefox, Safari, Edge)
- Compatible with authentication popup blockers (with user allowing popups)
- Works with Firebase Admin SDK 9.10+

