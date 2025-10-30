// app/static/auth.js
// ---- CONFIG
const COGNITO_DOMAIN = "video-transcoder-pair2.auth.ap-southeast-2.amazoncognito.com"; // your actual domain
const CLIENT_ID = "2r8jvq74v33dudedddvkiprct7";
const REDIRECT_URI = "http://localhost:3000/static/callback.html";

// ---------------------------------------------------------

const SCOPES = ["openid", "email", "aws.cognito.signin.user.admin"];
const RESPONSE_TYPE = "token"; // implicit flow -> returns access_token in URL fragment

function login() {
  const url = new URL(`https://${COGNITO_DOMAIN}/login`);
  url.searchParams.set("client_id", CLIENT_ID);
  url.searchParams.set("response_type", RESPONSE_TYPE);
  url.searchParams.set("scope", SCOPES.join(" "));
  url.searchParams.set("redirect_uri", REDIRECT_URI);
  window.location.assign(url.toString());
}

function logout() {
  // clear local token
  sessionStorage.removeItem("access_token");
  // bounce through Cognito logout to fully sign out
  const url = new URL(`https://${COGNITO_DOMAIN}/logout`);
  url.searchParams.set("client_id", CLIENT_ID);
  url.searchParams.set("logout_uri", window.location.origin + "/");
  window.location.assign(url.toString());
}

function getAccessToken() {
  return sessionStorage.getItem("access_token");
}

function isLoggedIn() {
  return !!getAccessToken();
}

async function apiFetch(path, options = {}) {
  const token = getAccessToken();
  const headers = new Headers(options.headers || {});
  if (token) headers.set("Authorization", `Bearer ${token}`);
  return fetch(path, { ...options, headers });
}

// Expose helpers globally for quick use in inline handlers
window.Auth = { login, logout, getAccessToken, isLoggedIn, apiFetch };
