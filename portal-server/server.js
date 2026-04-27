const express = require('express')
const session = require('express-session')
const crypto = require('crypto')
const path = require('path')
const fs = require('fs')
const http = require('http')
const httpProxy = require('http-proxy')
const { createProxyMiddleware } = require('http-proxy-middleware')
const FileStore = require('session-file-store')(session)

const app = express()
const server = http.createServer(app)
const PORT = process.env.PORT || 3001

// SHA-256 of '199887'
const PASSWORD_HASH = 'd220852e1f2aa3b2aa7a1e263774b2c16f8ba4fd7524aad67169df1d2d1259d0'
const SESSION_SECRET = process.env.SESSION_SECRET || 'sydney-portal-' + Math.random().toString(36)
const DIST_DIR = path.join(__dirname, 'dist')

function sha256(str) {
  return crypto.createHash('sha256').update(str).digest('hex')
}

app.set('trust proxy', 1)
app.use(express.json())
app.use(express.urlencoded({ extended: false }))
app.use(session({
  store: new FileStore({ path: '/opt/gutbiomedb/portal-server/sessions', ttl: 30*24*3600, reapInterval: 3600 }),
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    secure: 'auto',
    maxAge: 30 * 24 * 60 * 60 * 1000,
  },
}))

// Auth API
app.post('/api/auth/login', (req, res) => {
  const { password } = req.body
  if (!password) return res.status(400).json({ ok: false, error: 'missing password' })
  if (sha256(password) === PASSWORD_HASH) {
    req.session.authenticated = true
    return res.json({ ok: true })
  }
  return res.status(401).json({ ok: false, error: 'incorrect password' })
})

app.post('/api/auth/logout', (req, res) => {
  req.session.destroy()
  res.json({ ok: true })
})

app.get('/api/auth/status', (req, res) => {
  res.json({ authenticated: !!req.session.authenticated })
})

// Auth middleware
function requireAuth(req, res, next) {
  if (req.session.authenticated) return next()
  if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'unauthenticated' })
  res.sendFile(path.join(__dirname, 'login.html'))
}

app.use(requireAuth)

// System info API
const { execSync } = require('child_process')
app.get('/api/sysinfo', (req, res) => {
  try {
    const mem = execSync('free -b').toString().split('\n')[1].trim().split(/\s+/)
    const memTotal = parseInt(mem[1])
    const memUsed  = parseInt(mem[2])
    const df = execSync('df -B1 /').toString().split('\n')[1].trim().split(/\s+/)
    const diskTotal = parseInt(df[1])
    const diskUsed  = parseInt(df[2])
    const uptime = parseFloat(execSync('cat /proc/uptime').toString().split(' ')[0])
    res.json({ memTotal, memUsed, diskTotal, diskUsed, uptime })
  } catch (e) {
    res.status(500).json({ error: e.message })
  }
})

// FileBrowser proxy — noauth mode: auto-login to get JWT, then proxy with token
let fbToken = null
let fbTokenExpiry = 0

function getFbToken() {
  if (fbToken && Date.now() < fbTokenExpiry) return Promise.resolve(fbToken)
  return new Promise((resolve, reject) => {
    const body = Buffer.from('{}')
    const req = http.request({
      hostname: '127.0.0.1', port: 8090,
      path: '/api/login', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': body.length },
    }, (res) => {
      let data = ''
      res.on('data', c => data += c)
      res.on('end', () => {
        fbToken = data.trim()
        fbTokenExpiry = Date.now() + 20 * 60 * 60 * 1000
        resolve(fbToken)
      })
    })
    req.on('error', reject)
    req.write(body)
    req.end()
  })
}

app.use('/proxy/files', (req, res) => {
  const targetPath = req.path || '/'
  const query = req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : ''

  getFbToken().then(token => {
    const options = {
      hostname: '127.0.0.1',
      port: 8090,
      path: targetPath + query,
      method: req.method,
      headers: Object.assign(
        { 'X-Auth': token },
        req.headers['content-type'] && { 'Content-Type': req.headers['content-type'] },
        req.headers['content-length'] && { 'Content-Length': req.headers['content-length'] },
      ),
    }

    const proxyReq = http.request(options, (proxyRes) => {
      if (proxyRes.statusCode === 401) fbToken = null
      res.status(proxyRes.statusCode)
      Object.entries(proxyRes.headers).forEach(([k, v]) => {
        if (k.toLowerCase() !== 'transfer-encoding') res.setHeader(k, v)
      })
      proxyRes.pipe(res)
    })

    proxyReq.on('error', (err) => {
      res.status(502).json({ error: 'FileBrowser proxy error', detail: err.message })
    })

    if (req.method !== 'GET' && req.method !== 'HEAD') {
      req.pipe(proxyReq)
    } else {
      proxyReq.end()
    }
  }).catch(err => {
    res.status(502).json({ error: 'Cannot connect to FileBrowser', detail: err.message })
  })
})

// ttyd terminal proxy — raw http-proxy for reliable WebSocket bidirectional I/O
const wsProxy = httpProxy.createProxyServer({ target: 'http://127.0.0.1:7681', ws: true })

wsProxy.on('error', (err, req, res) => {
  if (res && typeof res.status === 'function') {
    res.status(502).json({ error: 'ttyd proxy error', detail: err.message })
  }
})

app.use('/proxy/terminal', (req, res) => {
  req.url = req.url.replace(/^\//, '') || '/'
  if (!req.url.startsWith('/')) req.url = '/' + req.url
  wsProxy.web(req, res)
})

server.on('upgrade', (req, socket, head) => {
  if (req.url.startsWith('/proxy/terminal')) {
    req.url = req.url.replace(/^\/proxy\/terminal/, '') || '/ws'
    if (!req.url.startsWith('/')) req.url = '/' + req.url
    wsProxy.ws(req, socket, head)
  }
})

// Serve React portal
if (fs.existsSync(DIST_DIR)) {
  app.use(express.static(DIST_DIR))
  app.get('*', (_req, res) => res.sendFile(path.join(DIST_DIR, 'index.html')))
} else {
  app.get('*', (_req, res) => res.send('<h2>Portal dist not built yet — run npm run build in portal/</h2>'))
}

server.listen(PORT, '127.0.0.1', () => {
  console.log(`Sydney Portal Server listening on http://127.0.0.1:${PORT}`)
})
