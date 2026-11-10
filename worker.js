// Cloady 保活 Worker + 控制面板（多节点版）
// 绑定：KV 命名空间，变量名 KV；Secret：PANEL_KEY
// 每个节点三层保活：根地址 GET / WebSocket 握手 / 流量下载（可选）

const MAX_NODES = 10;
const SUBREQ_BUDGET = 45; // 免费版每次调用最多 50 个子请求，留出余量

export default {
  async scheduled(_e, env, ctx) {
    ctx.waitUntil(run(env));
  },

  async fetch(req, env) {
    const u = new URL(req.url);
    if (!env.PANEL_KEY) return new Response("PANEL_KEY 未设置", { status: 500 });
    const token = await sha(env.PANEL_KEY);

    if (req.method === "POST" && u.pathname === "/login") {
      const f = await req.formData();
      if (String(f.get("pw") || "") !== env.PANEL_KEY) return loginPage("密码不对，再试一次");
      return new Response(null, {
        status: 303,
        headers: {
          Location: "/",
          "Set-Cookie": `pk=${token}; Max-Age=2592000; Path=/; HttpOnly; Secure; SameSite=Strict`,
        },
      });
    }

    const authed = (req.headers.get("Cookie") || "").split(/;\s*/).includes("pk=" + token);
    if (!authed) {
      if (req.method === "GET") return loginPage("");
      return new Response("forbidden", { status: 403 });
    }

    if (req.method === "POST" && u.pathname === "/config") {
      const b = await req.json();
      const cfg = await getCfg(env);

      if (b.link) {
        const p = parseLink(b.link);
        if (!p) return Response.json({ ok: false, error: "无法识别这条链接，请粘贴完整的 vless:// 链接" }, { status: 400 });
        const dup = cfg.nodes.find((n) => n.host === p.host && n.path === p.path);
        if (dup) {
          dup.net = p.net;
          if (b.name) dup.name = clean(b.name);
        } else {
          if (cfg.nodes.length >= MAX_NODES) return Response.json({ ok: false, error: `最多 ${MAX_NODES} 个节点` }, { status: 400 });
          cfg.nodes.push({
            id: crypto.randomUUID().slice(0, 8),
            name: clean(b.name) || `节点 ${cfg.nodes.length + 1}`,
            host: p.host, path: p.path, net: p.net, fileUrl: "", mb: 15,
          });
        }
      }
      if (b.del) cfg.nodes = cfg.nodes.filter((n) => n.id !== b.del);
      if (b.node) {
        const n = cfg.nodes.find((x) => x.id === b.node);
        if (n) {
          if (typeof b.name === "string" && clean(b.name)) n.name = clean(b.name);
          if (typeof b.fileUrl === "string") n.fileUrl = b.fileUrl.trim();
          if (typeof b.mb === "number") n.mb = Math.max(0, Math.min(50, b.mb));
        }
      }
      if (typeof b.notes === "string") cfg.notes = b.notes.slice(0, 2000);

      await env.KV.put("cfg", JSON.stringify(cfg));
      return Response.json({ ok: true });
    }

    if (req.method === "POST" && u.pathname === "/run") {
      return Response.json(await run(env));
    }

    const cfg = await getCfg(env);
    const log = JSON.parse((await env.KV.get("log")) || "[]");
    return new Response(page(cfg, log), {
      headers: { "content-type": "text/html;charset=utf-8", "cache-control": "no-store" },
    });
  },
};

function clean(s) {
  return String(s || "").trim().slice(0, 30);
}

async function getCfg(env) {
  const raw = JSON.parse((await env.KV.get("cfg")) || "{}");
  const cfg = { nodes: raw.nodes || [], notes: raw.notes || "" };
  if (!raw.nodes && raw.host) {
    cfg.nodes.push({
      id: "n1", name: "节点 1", host: raw.host, path: raw.path || "/",
      net: raw.net || "", fileUrl: raw.fileUrl || "", mb: raw.mb ?? 15,
    });
  }
  return cfg;
}

// 只保存主机和路径，UUID 不落盘
function parseLink(link) {
  const m = String(link).trim().match(/^vless:\/\/[^@]+@([^:\/?#]+)(?::\d+)?\?([^#]*)/i);
  if (!m) return null;
  const q = new URLSearchParams(m[2]);
  let path = q.get("path") || "/";
  try { path = decodeURIComponent(path); } catch (_) {}
  if (!path.startsWith("/")) path = "/" + path;
  return { host: m[1], path, net: q.get("type") || "tcp" };
}

async function run(env) {
  const cfg = await getCfg(env);
  const rec = { t: new Date().toISOString(), nodes: [], ok: false };
  const budget = { n: 0 };

  if (!cfg.nodes.length) {
    rec.nodes.push({ name: "配置", ok: false, steps: [{ name: "节点", ok: false, info: "还没有添加节点" }] });
  } else {
    for (const c of cfg.nodes) {
      const steps = [await stepRoot(c, budget), await stepWs(c, budget)];
      if (c.fileUrl && c.mb > 0) steps.push(await stepTraffic(c, budget));
      const web = steps.filter((s) => s.name !== "流量");
      const traffic = steps.find((s) => s.name === "流量");
      const ok = web.some((s) => s.ok) && (!traffic || traffic.ok);
      rec.nodes.push({ name: c.name, ok, steps });
    }
    rec.ok = rec.nodes.every((n) => n.ok);
  }

  const log = JSON.parse((await env.KV.get("log")) || "[]");
  log.unshift(rec);
  await env.KV.put("log", JSON.stringify(log.slice(0, 15)));
  return rec;
}

async function stepRoot(c, b) {
  try {
    b.n++;
    const r = await fetch(`https://${c.host}/`, { redirect: "manual", cf: { cacheTtl: 0 } });
    await r.arrayBuffer();
    return { name: "根地址", ok: true, info: "HTTP " + r.status };
  } catch (e) {
    return { name: "根地址", ok: false, info: String(e).slice(0, 80) };
  }
}

async function stepWs(c, b) {
  try {
    b.n++;
    const r = await fetch(`https://${c.host}${c.path}`, { headers: { Upgrade: "websocket" } });
    if (r.webSocket) {
      r.webSocket.accept();
      r.webSocket.close(1000, "bye");
      return { name: "WebSocket", ok: true, info: "握手成功 101" };
    }
    await r.arrayBuffer();
    return { name: "WebSocket", ok: true, info: "HTTP " + r.status };
  } catch (e) {
    return { name: "WebSocket", ok: false, info: String(e).slice(0, 80) };
  }
}

async function stepTraffic(c, b) {
  const want = c.mb * 1048576;
  let total = 0, n = 0;
  try {
    while (total < want && b.n < SUBREQ_BUDGET) {
      b.n++;
      const sep = c.fileUrl.includes("?") ? "&" : "?";
      const r = await fetch(`${c.fileUrl}${sep}r=${Math.random()}`, { cf: { cacheTtl: 0 } });
      if (!r.ok) return { name: "流量", ok: false, info: "HTTP " + r.status };
      total += (await r.arrayBuffer()).byteLength;
      n++;
    }
  } catch (e) {
    return { name: "流量", ok: false, info: String(e).slice(0, 80) };
  }
  const mb = (total / 1048576).toFixed(1);
  return { name: "流量", ok: total >= want, info: `${mb} MB / ${n} 次请求` };
}

function esc(s) {
  return String(s).replace(/[&<>"]/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[ch]));
}

const CSS = `
:root{--bg:#fbfaf7;--fg:#1d1d1b;--dim:#6b6a65;--line:#dcdad2;--ok:#1f7a4d;--bad:#b3261e;--card:#fff}
@media(prefers-color-scheme:dark){:root{--bg:#161715;--fg:#eceae4;--dim:#9a988f;--line:#33342f;--ok:#58c28a;--bad:#ff8a80;--card:#1e1f1c}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.6 system-ui,-apple-system,"Noto Sans SC",sans-serif}
main{max-width:520px;margin:0 auto;padding:20px 16px 48px}
h1{font-size:20px;margin:0 0 4px}h2{font-size:15px;margin:28px 0 8px}
.dim{color:var(--dim);font-size:14px}.ok{color:var(--ok)}.bad{color:var(--bad)}.warn{color:var(--bad);font-size:14px;margin:4px 0 0}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px;margin-bottom:10px}
input,textarea{width:100%;font:inherit;font-size:15px;padding:9px 10px;border:1px solid var(--line);border-radius:8px;background:var(--bg);color:var(--fg)}
textarea{min-height:76px;resize:vertical}
label{display:block;font-size:14px;color:var(--dim);margin:12px 0 4px}
button{font:inherit;font-size:15px;padding:9px 14px;border:1px solid var(--fg);border-radius:8px;background:transparent;color:var(--fg);margin:10px 8px 0 0}
button.main{background:var(--fg);color:var(--bg)}button.del{border-color:var(--bad);color:var(--bad)}
button:focus-visible,input:focus-visible,textarea:focus-visible,summary:focus-visible{outline:2px solid var(--ok);outline-offset:2px}
.hd{display:flex;justify-content:space-between;gap:8px;font-size:14px;margin-bottom:2px}.st{font-size:14px}
.rec{padding:10px 0;border-top:1px solid var(--line)}.rec:first-child{border-top:0}
.nd{margin-top:6px;padding-left:10px;border-left:2px solid var(--line)}
summary{cursor:pointer;font-size:14px;color:var(--dim);margin-top:8px}
.msg{min-height:22px;font-size:14px;margin-top:8px}
`;

function page(c, log) {
  const nodes =
    c.nodes
      .map((n) => {
        const warn =
          n.net && n.net !== "ws"
            ? `<p class=warn>传输方式是 ${esc(n.net)}，不是 ws，WebSocket 握手会失败，其余两层不受影响。</p>`
            : "";
        return `<div class=card>
<div class=hd><b>${esc(n.name)}</b><span class=dim>${esc(n.net || "未知")}</span></div>
<div class=dim>${esc(n.host)}<br>入口 ${esc(n.path.slice(0, 5))}…</div>${warn}
<details><summary>流量与改名（可选）</summary>
<label for="nm-${n.id}">名称</label><input id="nm-${n.id}" value="${esc(n.name)}">
<label for="f-${n.id}">流量文件地址（仅非 Web 应用需要，留空即可）</label>
<input id="f-${n.id}" value="${esc(n.fileUrl)}" placeholder="https://应用地址/test.bin">
<label for="m-${n.id}">每次下载 MB（填 0 关闭）</label>
<input id="m-${n.id}" type=number min=0 max=50 value="${esc(n.mb)}">
<button onclick="saveNode('${n.id}')">保存</button>
</details>
<button class=del onclick="delNode('${n.id}')">删除节点</button>
</div>`;
      })
      .join("") || `<p class=dim>还没有节点，在下面粘贴 vless:// 链接添加。</p>`;

  const rows =
    log
      .map(
        (l) => `<div class=rec><div class=hd><span>${esc(l.t.slice(5, 16).replace("T", " "))} UTC</span><b class="${l.ok ? "ok" : "bad"}">${l.ok ? "全部成功" : "有失败"}</b></div>${(l.nodes || [])
          .map(
            (n) => `<div class=nd><b class="${n.ok ? "ok" : "bad"}">${esc(n.name)}</b>${n.steps
              .map((s) => `<div class=st><span class="${s.ok ? "ok" : "bad"}">${s.ok ? "通过" : "失败"}</span> ${esc(s.name)} <span class=dim>${esc(s.info)}</span></div>`)
              .join("")}</div>`
          )
          .join("")}</div>`
      )
      .join("") || `<p class=dim>还没有运行记录，点“立即运行”试一次。</p>`;

  return `<!doctype html><html lang=zh><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Cloady 保活</title>
<style>${CSS}</style>
<main>
<h1>Cloady 保活</h1>
<p class=dim>每 3 天自动运行一次，所有节点依次保活。已添加 ${c.nodes.length} / ${MAX_NODES} 个节点。</p>

<h2>节点</h2>
${nodes}

<h2>添加节点</h2>
<div class=card>
<label for=name>名称（可选）</label>
<input id=name placeholder="例如 德国一号">
<label for=link>粘贴 vless:// 链接，自动识别主机和路径（UUID 不会保存）</label>
<textarea id=link placeholder="vless://…"></textarea>
<button class=main onclick="addNode()">添加节点</button>
<div class=msg id=msg></div>
</div>

<h2>账号备注</h2>
<div class=card>
<label for=notes>记录账号邮箱、工作区、到期日等。不建议写密码，备注是明文保存的。</label>
<textarea id=notes style="min-height:120px">${esc(c.notes || "")}</textarea>
<button onclick="saveNotes()">保存备注</button>
<div class=msg id=m2></div>
</div>

<h2>运行</h2>
<button class=main onclick="go()">立即运行</button>

<h2>最近记录</h2>
<div class=card>${rows}</div>
</main>
<script>
const $=(i)=>document.getElementById(i);
async function post(b){const r=await fetch("/config",{method:"POST",body:JSON.stringify(b)});return r.json();}
async function addNode(){
  if(!$("link").value.trim()){$("msg").textContent="先粘贴一条 vless:// 链接";return;}
  const d=await post({link:$("link").value,name:$("name").value});
  if(d.ok){location.reload();}else{$("msg").textContent=d.error||"添加失败";}
}
async function saveNode(id){
  const d=await post({node:id,name:$("nm-"+id).value,fileUrl:$("f-"+id).value,mb:Number($("m-"+id).value)||0});
  if(d.ok){location.reload();}
}
async function delNode(id){
  if(!confirm("确定删除这个节点吗？")) return;
  const d=await post({del:id});
  if(d.ok){location.reload();}
}
async function saveNotes(){
  const d=await post({notes:$("notes").value});
  $("m2").textContent=d.ok?"备注已保存":"保存失败";
}
async function go(){
  await fetch("/run",{method:"POST"});
  location.reload();
}
</script></html>`;
}

async function sha(text) {
  const d = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

function loginPage(err) {
  const html = `<!doctype html><html lang=zh><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Cloady 保活</title>
<style>
:root{--bg:#fbfaf7;--fg:#1d1d1b;--dim:#6b6a65;--line:#dcdad2;--bad:#b3261e}
@media(prefers-color-scheme:dark){:root{--bg:#161715;--fg:#eceae4;--dim:#9a988f;--line:#33342f;--bad:#ff8a80}}
body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.6 system-ui,-apple-system,"Noto Sans SC",sans-serif}
main{max-width:360px;margin:20vh auto 0;padding:0 16px}
h1{font-size:20px;margin:0 0 12px}
input{width:100%;box-sizing:border-box;font:inherit;padding:10px;border:1px solid var(--line);border-radius:8px;background:var(--bg);color:var(--fg)}
button{font:inherit;margin-top:12px;padding:10px 16px;border:1px solid var(--fg);border-radius:8px;background:var(--fg);color:var(--bg)}
p{color:var(--bad);font-size:14px;min-height:22px}
</style>
<main><h1>Cloady 保活</h1>
<form method=post action=/login>
<input type=password name=pw placeholder="面板密码" autocomplete=current-password autofocus>
<button>登录</button></form><p>${esc(err)}</p></main></html>`;
  return new Response(html, {
    status: err ? 401 : 200,
    headers: { "content-type": "text/html;charset=utf-8", "cache-control": "no-store" },
  });
}
