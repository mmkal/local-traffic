"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __rest = (this && this.__rest) || function (s, e) {
    var t = {};
    for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)
        t[p] = s[p];
    if (s != null && typeof Object.getOwnPropertySymbols === "function")
        for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
            if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i]))
                t[p[i]] = s[p[i]];
        }
    return t;
};
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
exports.update = exports.serve = exports.determineMapping = exports.send = exports.cleanEntropy = exports.replaceTextUsingMapping = exports.replaceBody = exports.acknowledgeWebsocket = exports.readWebsocketBuffer = exports.createWebsocketBufferFrom = exports.websocketServe = exports.recorderHandler = exports.quickStatus = exports.errorListener = exports.load = exports.start = void 0;
const http2_1 = require("http2");
const http_1 = require("http");
const https_1 = require("https");
const url_1 = require("url");
const fs_1 = require("fs");
const zlib_1 = require("zlib");
const path_1 = require("path");
const crypto_1 = require("crypto");
const process_1 = require("process");
const os_1 = require("os");
var LogLevel;
(function (LogLevel) {
    LogLevel[LogLevel["ERROR"] = 124] = "ERROR";
    LogLevel[LogLevel["INFO"] = 93] = "INFO";
    LogLevel[LogLevel["WARNING"] = 172] = "WARNING";
})(LogLevel || (LogLevel = {}));
var EMOJIS;
(function (EMOJIS) {
    EMOJIS["INBOUND"] = "\u2198\uFE0F ";
    EMOJIS["PORT"] = "\u260E\uFE0F ";
    EMOJIS["OUTBOUND"] = "\u2197\uFE0F ";
    EMOJIS["RULES"] = "\uD83D\uDD17";
    EMOJIS["MOCKS"] = "\uD83C\uDF10";
    EMOJIS["STRICT_MOCKS"] = "\uD83D\uDD78\uFE0F";
    EMOJIS["AUTO_RECORD"] = "\uD83D\uDCFC";
    EMOJIS["REWRITE"] = "\u2712\uFE0F ";
    EMOJIS["LOGS"] = "\uD83D\uDCDD";
    EMOJIS["RESTART"] = "\uD83D\uDD04";
    EMOJIS["WEBSOCKET"] = "\u2604\uFE0F ";
    EMOJIS["COLORED"] = "\u2728";
    EMOJIS["SHIELD"] = "\uD83D\uDEE1\uFE0F ";
    EMOJIS["NO"] = "\u26D4";
    EMOJIS["ERROR_1"] = "\u274C";
    EMOJIS["ERROR_2"] = "\u26C8\uFE0F ";
    EMOJIS["ERROR_3"] = "\u2622\uFE0F ";
    EMOJIS["ERROR_4"] = "\u2049\uFE0F ";
    EMOJIS["ERROR_5"] = "\u26A1";
    EMOJIS["ERROR_6"] = "\u2620\uFE0F ";
})(EMOJIS || (EMOJIS = {}));
var REPLACEMENT_DIRECTION;
(function (REPLACEMENT_DIRECTION) {
    REPLACEMENT_DIRECTION["INBOUND"] = "INBOUND";
    REPLACEMENT_DIRECTION["OUTBOUND"] = "OUTBOUND";
})(REPLACEMENT_DIRECTION || (REPLACEMENT_DIRECTION = {}));
var ServerMode;
(function (ServerMode) {
    ServerMode["PROXY"] = "proxy";
    ServerMode["MOCK"] = "mock";
})(ServerMode || (ServerMode = {}));
const mainProgram = require.main === module
    ? __filename
    : (_a = process_1.argv
        .map(arg => arg.trim())
        .filter(arg => arg &&
        !["ts-node", "node", "npx", "npm", "exec"].some(pattern => arg.includes(pattern) &&
            !arg.match(/npm-cache/) &&
            !arg.match(/_npx/)))[0]) !== null && _a !== void 0 ? _a : "";
const runAsMainProgram = mainProgram.toLowerCase().replace(/[-_]/g, "").includes("localtraffic") &&
    !mainProgram.match(/(.|-)?(test|spec)\.m?[jt]sx?$/);
const filename = !runAsMainProgram
    ? `${(0, os_1.tmpdir)()}${path_1.sep}local-traffic-temporary-config-${(0, crypto_1.randomBytes)(6).toString("hex")}.json`
    : (0, path_1.resolve)((0, process_1.cwd)(), process_1.argv.slice(-1)[0].endsWith(".json")
        ? process_1.argv.slice(-1)[0]
        : (0, path_1.resolve)((0, os_1.homedir)(), ".local-traffic.json"));
const crashTest = process_1.argv.some(arg => arg === "--crash-test");
const screenWidth = 64;
const instantTime = () => {
    var _a, _b;
    return ((_b = (_a = process_1.hrtime.bigint) === null || _a === void 0 ? void 0 : _a.call(process_1.hrtime)) !== null && _b !== void 0 ? _b : (() => {
        const time = (0, process_1.hrtime)();
        return (time[0] * 1000 + time[1] / 1000000);
    })());
};
const getCurrentTime = (simpleLogs) => {
    const date = new Date();
    return `${simpleLogs ? "" : "\u001b[36m"}${`${date.getHours()}`.padStart(2, "0")}${simpleLogs ? ":" : "\u001b[33m:\u001b[36m"}${`${date.getMinutes()}`.padStart(2, "0")}${simpleLogs ? ":" : "\u001b[33m:\u001b[36m"}${`${date.getSeconds()}`.padStart(2, "0")}${simpleLogs ? "" : "\u001b[0m"}`;
};
const levelToString = (level) => level === LogLevel.ERROR
    ? "error"
    : level === LogLevel.WARNING
        ? "warning"
        : "info";
const log = function (state, logs) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a, _b, _c, _d, _e, _f, _g;
        const simpleTexts = logs.map(logLine => logLine
            .map(e => e.text
            .replace(/⎸/g, "|")
            .replace(/⎹/g, "|")
            .replace(/\u001b\[[^m]*m/g, "")
            .replace(new RegExp(EMOJIS.INBOUND, "g"), "inbound:")
            .replace(new RegExp(EMOJIS.PORT, "g"), "port:")
            .replace(new RegExp(EMOJIS.OUTBOUND, "g"), "outbound:")
            .replace(new RegExp(EMOJIS.RULES, "g"), "rules:")
            .replace(new RegExp(EMOJIS.NO, "g"), "")
            .replace(new RegExp(EMOJIS.REWRITE, "g"), "+rewrite")
            .replace(new RegExp(EMOJIS.WEBSOCKET, "g"), "websocket")
            .replace(new RegExp(EMOJIS.SHIELD, "g"), "web-security")
            .replace(new RegExp(EMOJIS.MOCKS, "g"), "mocks")
            .replace(new RegExp(EMOJIS.STRICT_MOCKS, "g"), "mocks (strict)")
            .replace(new RegExp(EMOJIS.AUTO_RECORD, "g"), "auto record")
            .replace(new RegExp(EMOJIS.LOGS, "g"), "logs")
            .replace(new RegExp(EMOJIS.RESTART, "g"), "restart")
            .replace(new RegExp(EMOJIS.COLORED, "g"), "colored")
            .replace(/\|+/g, "|"))
            .join(" | "));
        if ((_a = state === null || state === void 0 ? void 0 : state.config) === null || _a === void 0 ? void 0 : _a.simpleLogs)
            for (let simpleText of simpleTexts)
                process_1.stdout.write(`${getCurrentTime((_b = state === null || state === void 0 ? void 0 : state.config) === null || _b === void 0 ? void 0 : _b.simpleLogs)} | ${simpleText}\n`);
        else {
            for (let element of logs) {
                const renderedColors = element.filter(e => { var _a; return (_a = e === null || e === void 0 ? void 0 : e.text) === null || _a === void 0 ? void 0 : _a.length; });
                const logTexts = renderedColors.map(e => `\u001b[48;5;${e.color}m${e.text}`);
                process_1.stdout.write(`${getCurrentTime((_c = state === null || state === void 0 ? void 0 : state.config) === null || _c === void 0 ? void 0 : _c.simpleLogs)}${renderedColors
                    .map(e => { var _a; return `\u001b[48;5;${e.color}m${"".padEnd(((_a = e.length) !== null && _a !== void 0 ? _a : screenWidth) + 1)}`; })
                    .join("▐")}\u001b[0m\n`);
                yield new Promise(resolve => process_1.stdout.moveCursor(-1000, -1, () => resolve(void 0)));
                let offset = 9;
                for (let i = 0; i < logTexts.length; i++) {
                    yield new Promise(resolve => process_1.stdout.moveCursor(-1000, 0, () => process_1.stdout.moveCursor(offset, 0, () => resolve(void 0))));
                    process_1.stdout.write(logTexts[i]);
                    offset += ((_d = element[i].length) !== null && _d !== void 0 ? _d : screenWidth) + 2;
                }
                process_1.stdout.write("\u001b[0m\n");
                for (let simpleText of simpleTexts)
                    (_e = state === null || state === void 0 ? void 0 : state.notifyLogsListeners) === null || _e === void 0 ? void 0 : _e.call(state, {
                        event: simpleText,
                        level: levelToString((_g = (_f = element === null || element === void 0 ? void 0 : element[0]) === null || _f === void 0 ? void 0 : _f.color) !== null && _g !== void 0 ? _g : LogLevel.INFO),
                    });
            }
        }
    });
};
const createWebsocketBufferFrom = (text, wantsMask) => {
    var _a;
    const mask = Array(4)
        .fill(0)
        .map(() => (wantsMask ? Math.floor(Math.random() * (2 << 7)) : 0));
    const maskedTextBits = text
        .split("")
        .map((c, i) => c.charCodeAt(0) ^ mask[i & 3]);
    const length = text.length;
    const magicHeader = (1 << 7) + 1;
    const maskHeader = wantsMask ? 1 << 7 : 0;
    const header = text.length < (2 << 6) - 2
        ? Buffer.from(Uint8Array.from([magicHeader, maskHeader + length]).buffer)
        : text.length < (2 << 15) - 1
            ? Buffer.concat([
                Buffer.from(Uint8Array.from([magicHeader, ((1 << 7) - 2) | maskHeader])
                    .buffer),
                Buffer.from(Uint8Array.from([length >> 8]).buffer),
                Buffer.from(Uint8Array.from([length & ((1 << 8) - 1)]).buffer),
            ])
            : Buffer.concat([
                Buffer.from(Uint8Array.from([magicHeader, ((1 << 7) - 1) | maskHeader])
                    .buffer),
                Buffer.concat(((_a = Number(length)
                    .toString(16)
                    .padStart(16, "0")
                    .match(/.{2}/g)) !== null && _a !== void 0 ? _a : ["0"])
                    .map(e => parseInt(e, 16))
                    .map(number => Buffer.from(Uint8Array.from([number]).buffer))),
            ]);
    const maskingKey = Buffer.from(Int8Array.from(mask).buffer);
    const payload = Buffer.from(Int8Array.from(maskedTextBits).buffer);
    return Buffer.concat(wantsMask ? [header, maskingKey, payload] : [header, payload]);
};
exports.createWebsocketBufferFrom = createWebsocketBufferFrom;
const readWebsocketBuffer = (buffer, partialRead) => {
    var _a;
    if (!partialRead && (buffer.readUInt8(0) & 1) === 0)
        return { payloadLength: 0, mask: [0, 0, 0, 0], body: "" };
    const headerSecondByte = partialRead ? 0 : buffer.readUInt8(1);
    const hasMask = headerSecondByte >> 7;
    const payloadLengthFirstByte = headerSecondByte & ((1 << 7) - 1);
    const payloadLength = partialRead
        ? partialRead.payloadLength
        : payloadLengthFirstByte !== (1 << 7) - 1
            ? payloadLengthFirstByte
            : buffer.readUInt8(2) << (8 + buffer.readUInt8(3));
    const mask = partialRead
        ? partialRead.mask
        : !hasMask
            ? [0, 0, 0, 0]
            : Array(4)
                .fill(0)
                .map((_, i) => buffer.readUInt8(i + 4));
    const payloadStart = partialRead ? 0 : hasMask ? 8 : 4;
    const body = Array(buffer.length - payloadStart)
        .fill(0)
        .map((_, i) => String.fromCharCode(buffer.readUInt8(i + payloadStart) ^ mask[i & 3]))
        .join("");
    return { payloadLength, mask, body: ((_a = partialRead === null || partialRead === void 0 ? void 0 : partialRead.body) !== null && _a !== void 0 ? _a : "").concat(body) };
};
exports.readWebsocketBuffer = readWebsocketBuffer;
const acknowledgeWebsocket = (socket, key) => {
    const shasum = (0, crypto_1.createHash)("sha1");
    shasum.update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    const accept = shasum.digest("base64");
    socket.allowHalfOpen = true;
    socket.write("HTTP/1.1 101 Switching Protocols\r\n" +
        `date: ${new Date().toUTCString()}\r\n` +
        "connection: upgrade\r\n" +
        "upgrade: websocket\r\n" +
        "server: local\r\n" +
        `sec-websocket-accept: ${accept}\r\n` +
        "\r\n");
};
exports.acknowledgeWebsocket = acknowledgeWebsocket;
const notifyConfigListeners = function (data) {
    return notifyListeners(data, this.configListeners);
};
const notifyLogsListeners = function (data) {
    const { response } = data, dataWithoutResponseBody = __rest(data, ["response"]);
    return Promise.all([
        notifyListeners(data, this.logsListeners.filter(l => l.wantsResponseMessage)),
        notifyListeners(dataWithoutResponseBody, this.logsListeners.filter(l => !l.wantsResponseMessage)),
    ]);
};
const notifyListeners = (data, listeners) => {
    if (!listeners.length)
        return;
    const text = JSON.stringify(data);
    const wantsMask = new Set(listeners.map(listener => listener.wantsMask));
    const bufferWithoutMask = wantsMask.has(false) && createWebsocketBufferFrom(text, false);
    const bufferWithMask = wantsMask.has(true) && createWebsocketBufferFrom(text, true);
    const streamError = (listener) => {
        if (!listener.stream.errored)
            return;
        listener.stream.destroy();
    };
    listeners.forEach(listener => {
        if (listener.stream.closed || listener.stream.errored)
            return;
        listener.wantsMask
            ? listener.stream.write(bufferWithMask, "ascii", () => streamError(listener))
            : listener.stream.write(bufferWithoutMask, "ascii", () => streamError(listener));
    });
};
const buildQuickStatus = function () {
    var _a, _b;
    return [
        {
            color: 52,
            text: `${EMOJIS.PORT} ${((_a = this.config.port) !== null && _a !== void 0 ? _a : "").toString()}`,
            length: 11,
        },
        {
            color: 53,
            text: `${EMOJIS.OUTBOUND} ${this.config.dontUseHttp2Downstream ? "H1.1" : "H/2 "}${this.config.replaceRequestBodyUrls ? EMOJIS.REWRITE : "  "}`,
            length: 11,
        },
        {
            color: 54,
            text: `${EMOJIS.INBOUND} ${this.config.ssl ? "H/2 " : "H1.1"}${this.config.replaceResponseBodyUrls ? EMOJIS.REWRITE : "  "}`,
            length: 11,
        },
        {
            color: 55,
            text: `${this.mode === ServerMode.PROXY && this.mockConfig.autoRecord
                ? `${EMOJIS.AUTO_RECORD}${this.mockConfig.mocks.size
                    .toString()
                    .padStart(3)}`
                : this.mode === ServerMode.PROXY
                    ? `${EMOJIS.RULES}${Object.keys((_b = this.config.mapping) !== null && _b !== void 0 ? _b : {})
                        .length.toString()
                        .padStart(3)}`
                    : `${this.mockConfig.strict ? EMOJIS.STRICT_MOCKS : EMOJIS.MOCKS}${this.mockConfig.mocks.size.toString().padStart(3)}`}`,
            length: 7,
        },
        {
            color: 56,
            text: `${this.config.websocket ? EMOJIS.WEBSOCKET : EMOJIS.NO}`,
            length: 4,
        },
        {
            color: 57,
            text: `${!this.config.simpleLogs ? EMOJIS.COLORED : EMOJIS.NO}`,
            length: 4,
        },
        {
            color: 93,
            text: `${this.config.disableWebSecurity ? EMOJIS.NO : EMOJIS.SHIELD}`,
            length: 4,
        },
    ];
};
const quickStatus = function (otherLogElements) {
    return __awaiter(this, void 0, void 0, function* () {
        this.log([...(otherLogElements !== null && otherLogElements !== void 0 ? otherLogElements : []), this.buildQuickStatus()]).then(() => this.notifyConfigListeners(this.config));
    });
};
exports.quickStatus = quickStatus;
const errorPage = (thrown, serverMode, phase, requestedURL, downstreamURL) => `${header(0x1f4a3, "error", thrown.message)}
<p>An error happened while trying to proxy a remote exchange</p>
<div class="alert alert-warning" role="alert">
  &#x24D8;&nbsp;This is not an error from the downstream service.
</div>
<div class="alert alert-danger" role="alert">
<pre><code>${thrown.stack || `<i>${thrown.name} : ${thrown.message}</i>`}${thrown.errno
    ? `<br/>(code : ${thrown.errno})`
    : ""}</code></pre>
</div>
More information about the request :
<table class="table">
  <tbody>
    <tr>
      <td>server mode</td>
      <td>${serverMode}</td>
    </tr>
    <tr>
      <td>phase</td>
      <td>${phase}</td>
    </tr>
    <tr>
      <td>requested URL</td>
      <td>${requestedURL}</td>
    </tr>
    <tr>
      <td>downstream URL</td>
      <td>${downstreamURL || "&lt;no-target-url&gt;"}</td>
    </tr>
  </tbody>
</table>
</div></body></html>`;
const logsView = (proxyHostnameAndPort, config, options) => `<table id="table-access" class="table table-striped" style="display: block; width: 100%; overflow-y: auto">
  <thead>
    <tr>
      <th scope="col"${options.captureResponseBody === true ? ' style="min-width: 120px"' : ""}>...</th>
      <th scope="col">Date</th>
      <th scope="col">Level</th>
      <th scope="col">Protocol</th>
      <th scope="col">Method</th>
      <th scope="col">Status</th>
      <th scope="col">Duration</th>
      <th scope="col">Upstream Path</th>
      <th scope="col">Downstream Path</th>
    </tr>
  </thead>
  <tbody id="access">
  </tbody>
</table>
<table id="table-proxy" class="table table-striped" style="display: none; width: 100%; overflow-y: auto">
  <thead>
    <tr>
      <th scope="col">Date</th>
      <th scope="col">Level</th>
      <th scope="col">Message</th>
    </tr>
  </thead>
  <tbody id="proxy">
  </tbody>
</table>
<div class="alert alert-warning" role="alert"
style="display:none;left:20%;right:20%;top:20%;position:absolute;z-index:1;"
id="websocket-disconnected">
<p>&#x24D8;&nbsp;Websocket connection is not available at this moment.</p>
<ul><li>Is local-traffic running ?</li><li>Are websockets enabled ?</li>
<li>Are you running a network protection tool that disallows websockets ?</li></ul>
</div>
<script type="text/javascript">
    let socket = null;
    function start() {
      document.getElementById('table-access').style.height =
        (document.documentElement.clientHeight - 150) + 'px';
      if (socket !== null) return;
      socket = new WebSocket("ws${config.ssl ? "s" : ""}://${proxyHostnameAndPort}/local-traffic-logs${options.captureResponseBody ? "?wantsResponseMessage=true" : ""}");
      socket.onopen = function(event) {
        document.getElementById('websocket-disconnected').style.display = 'none';
        document.getElementById('table-access').style.filter = null;
        (document.getElementsByTagName('nav')[0]||{style:{}}).style.filter = null;
        (document.getElementsByTagName('form')[0]||{style:{}}).style.filter = null;
      }
      socket.onmessage = function(event) {
        let data = event.data
        let uniqueHash;
        try {
          const { uniqueHash: uniqueHash1, ...data1 } = JSON.parse(event.data);
          data = data1;
          uniqueHash = uniqueHash1;
        } catch(e) { }
        if (document.getElementById('mock-mode')?.checked) return;
        if (${options.captureResponseBody === true} && 
          data?.downstreamPath?.startsWith('recorder://') &&
          !data?.upstreamPath?.endsWith('?forceLogInRecorderPage=true'))
          return;
        const time = new Date().toISOString().split('T')[1].replace('Z', '');
        const actions = getActionsHtmlText(uniqueHash, data.response);
        if(data.statusCode && uniqueHash) {
          const color = getColorFromStatusCode(data.statusCode);
          const statusCodeColumn = document.querySelector("#event-" + data.randomId + " .statusCode");
          if (statusCodeColumn)
            statusCodeColumn.innerHTML = '<span class="badge bg-' + color + '">' + data.statusCode + '</span>';

          const durationColumn = document.querySelector("#event-" + data.randomId + " .duration");
          if (durationColumn) {
            const duration = data.duration > 10000 ? Math.floor(data.duration / 1000) + 's' :
              data.duration + 'ms';
            durationColumn.innerHTML = duration;
          }

          const protocolColumn = document.querySelector("#event-" + data.randomId + " .protocol");
          if (protocolColumn) {
            protocolColumn.innerHTML = data.protocol;
          }

          const replayColumn = document.querySelector("#event-" + data.randomId + " .replay");
          if (replayColumn) {
            replayColumn.innerHTML = actions;
          }
        } else if (uniqueHash) {
          addNewRequest(data.randomId, actions, time, data.level, data.protocol, data.method, 
            '<span class="badge bg-secondary">...</span>', '&#x23F1;',
            data.upstreamPath, data.downstreamPath);
        } else if(data.event) {
          document.getElementById("proxy")
            .insertAdjacentHTML('afterbegin', '<tr><td scope="col">' + time + '</td>' +
                '<td scope="col">' + (data.level || 'info')+ '</td>' + 
                '<td scope="col">' + data.event + '</td></tr>');
        }
        cleanup();
      };
      socket.onerror = function(error) {
        socket = null;
        setTimeout(start, 1000);
        if (error.target.readyState === 3) {
          document.getElementById('websocket-disconnected').style.display = 'block';
          document.getElementById('table-access').style.filter = 'blur(8px)';
          (document.getElementsByTagName('nav')[0]||{style:{}}).style.filter = 'blur(8px)';
          (document.getElementsByTagName('form')[0]||{style:{}}).style.filter = 'blur(8px)';
          return;
        }
        throw new Error(\`[error] \${JSON.stringify(error)}\`);
      };
      socket.onclose = function(error) {
        socket = null;
        setTimeout(start, 1000);
      };
    };
    function show(id) {
      [...document.querySelectorAll('table')].forEach((table, index) => {
        table.style.display = index === id ? 'block': 'none'
      });
      [...document.querySelectorAll('.navbar-nav .nav-item .nav-link')].forEach((link, index) => {
        if (index === id) { link.classList.add('active') } else link.classList.remove('active');
      });
    }
    function remove(event) {
      event.target.closest('tr').remove();
      if (window.updateState) window.updateState();
    }
    function cleanup() {
      const currentLimit = parseInt(document.getElementById('limit').value)
      for (let table of ['access', 'proxy']) {
        while (currentLimit && document.getElementById(table).childNodes.length && 
        document.getElementById(table).childNodes.length > currentLimit) {
          [...document.getElementById(table).childNodes].slice(-1)[0].remove();
        }
      }
    }
    function replay(event) {
      const uniqueHash = event.target.dataset.uniquehash;
      const { method, url, headers, body } = JSON.parse(atob(uniqueHash));
      fetch(url, {
        method,
        headers,
        body: !body || !body.length ? undefined : atob(body)
      });
    }
    function getActionsHtmlText(uniqueHash, response) {
      const edit = ${options.captureResponseBody === true} && uniqueHash
      ? '<button data-response="' + (response ?? "") +
      '" data-uniquehash="' + uniqueHash + 
      '" data-bs-toggle="modal" data-bs-target="#edit-request" type="button" ' +
        'class="btn btn-primary">&#x1F4DD;</button>'
      : ''
      const remove = ${options.captureResponseBody === true} && uniqueHash
      ? '<button onclick="javascript:remove(event)" type="button" ' +
        'class="btn btn-primary">&#x274C;</button>'
      : ''
      const replay = ${options.captureResponseBody === false} && uniqueHash ? '<button data-response="' + 
        btoa(JSON.stringify(response ?? {})) +
        '" data-uniquehash="' + uniqueHash + '" onclick="javascript:replay(event)" ' +
        'type="button" class="btn btn-primary">&#x1F501;</button>' : '';
      return edit + replay + remove
    }
    function addNewRequest(
      randomId, actions, time, level, protocol, method, 
      statusCode, duration, upstreamPath, downstreamPath
    ) {
      document.getElementById("access")
      .insertAdjacentHTML('afterbegin', '<tr id="event-' + randomId + '">' +
      '<td scope="col" class="replay">' + actions + '</td>' +
      '<td scope="col">' + time + '</td>' +
      '<td scope="col">' + (level || 'info')+ '</td>' + 
      '<td scope="col" class="protocol">' + protocol + '</td>' + 
      '<td scope="col" class="method">' + method + '</td>' + 
      '<td scope="col" class="statusCode">' + statusCode + '</td>' +
      '<td scope="col" class="duration text-end">' + duration + '</td>' +
      '<td scope="col" class="upstream-path">' + upstreamPath + '</td>' + 
      '<td scope="col">' + 
      ((downstreamPath??'').startsWith('data:') ? 'data:...' : downstreamPath) + 
      '</td>' + 
      '</tr>');
    }
    function getColorFromStatusCode(statusCode) {
      return Math.floor(statusCode / 100) === 1 ? "info" :
        Math.floor(statusCode / 100) === 2 ? "success" :
        Math.floor(statusCode / 100) === 3 ? "dark" :
        Math.floor(statusCode / 100) === 4 ? "warning" :
        Math.floor(statusCode / 100) === 5 ? "danger" :
        "secondary";
    }
    window.addEventListener("DOMContentLoaded", start);
</script>`;
const logsPage = (state, _, mappingAttributes) => staticResponse(`${header(0x1f4fa, "logs", "")}
<nav class="navbar navbar-expand-lg navbar-dark bg-primary nav-fill">
  <div class="container-fluid">
    <ul class="navbar-nav">
      <li class="nav-item">
        <a class="nav-link active" aria-current="page" href="javascript:show(0)">Access</a>
      </li>
      <li class="nav-item">
        <a class="nav-link" href="javascript:show(1)">Proxy</a>
      </li>
    </ul>
    <span class="navbar-text">
      Limit : <select id="limit" onchange="javascript:cleanup()"><option value="-1">0 (clear)</option><option value="10">10</option>
        <option value="50">50</option><option value="100">100</option><option value="200">200</option>
        <option selected="selected" value="500">500</option><option value="0">Infinity (discouraged)</option>
      </select> rows
    </span>
  </div>
</nav>
${logsView(mappingAttributes.proxyHostnameAndPort, state.config, {
    captureResponseBody: false,
})}
</body></html>`);
const configPage = (state, request, mappingAttributes) => {
    var _a, _b, _c, _d, _e, _f;
    if (["POST", "PUT"].includes(request.method)) {
        let newConfig;
        try {
            newConfig = JSON.parse((_b = (_a = mappingAttributes === null || mappingAttributes === void 0 ? void 0 : mappingAttributes.requestBody) === null || _a === void 0 ? void 0 : _a.toString("ascii")) !== null && _b !== void 0 ? _b : "{}");
        }
        catch (e) {
            setTimeout(() => state.log([
                [
                    {
                        text: `${EMOJIS.ERROR_4} config update could not be read`,
                        color: LogLevel.WARNING,
                    },
                ],
            ]), 1);
            return staticResponse(`{"error":"config update could not be read","stack":"${(_d = (_c = e.stack) === null || _c === void 0 ? void 0 : _c.replace) === null || _d === void 0 ? void 0 : _d.call(_c, /"/g, '\\"').replace(/[\s]+/g, " ")}"}`, {
                headers: { contentType: "application/json; charset=utf-8" },
            });
        }
        return staticResponse(new Promise(resolve => {
            var _a, _b;
            (_b = (_a = state.configFileWatcher) === null || _a === void 0 ? void 0 : _a.once) === null || _b === void 0 ? void 0 : _b.call(_a, "change", () => {
                setTimeout(() => {
                    // state.config has mutated (maybe) in function 'update'
                    resolve(Buffer.from(JSON.stringify(state.config)));
                }, 10);
            });
            update(state, { pendingConfigSave: newConfig });
        }), {
            headers: { contentType: "application/json; charset=utf-8" },
        });
    }
    if (["GET", "HEAD"].includes(request.method) &&
        ((_f = (_e = request.headers) === null || _e === void 0 ? void 0 : _e["accept"]) === null || _f === void 0 ? void 0 : _f.includes("application/json"))) {
        return staticResponse(JSON.stringify(state.config), {
            headers: { contentType: "application/json; charset=utf-8" },
        });
    }
    return staticResponse(`${header(0x1f39b, "config", "")}
    <link href="${cdn}jsoneditor/dist/jsoneditor.min.css" rel="stylesheet" type="text/css">
    <script src="${cdn}jsoneditor/dist/jsoneditor.min.js"></script>
    <script src="${cdn}node-forge/dist/forge.min.js"></script>
    <div id="ssl-modal" class="modal" tabindex="-1" role="dialog">
      <div class="modal-dialog" role="document">
        <div class="modal-content">
          <div class="modal-header">
            <h5 class="modal-title">SSL keypair generation in progress</h5>
          </div>
          <div class="modal-body">
            <p>Wait a few seconds or move your mouse to increase the entropy.</p>
          </div>
        </div>
      </div>
    </div>
    <div id="jsoneditor" style="width: 400px; height: 400px;"></div>
    <script>
    let socket = null;
    const container = document.getElementById("jsoneditor")
    const options = {mode: "code", allowSchemaSuggestions: true, schema: {
      type: "object",
      properties: {
        ${Object.entries(Object.assign(Object.assign({}, defaultConfig), { ssl: { cert: "", key: "" } }))
        .map(([property, exampleValue]) => `${property}:${property === "unwantedHeaderNamesInMocks"
        ? '{type:"array","items":{"type":"string"}}'
        : property === "logAccessInTerminal"
            ? '{"oneOf":[{type:"boolean"},{enum:["with-mapping"]}]}'
            : typeof exampleValue === "number"
                ? '{type:"integer"}'
                : typeof exampleValue === "string"
                    ? '{type:"string"}'
                    : typeof exampleValue === "boolean"
                        ? '{type:"boolean"}'
                        : '{type:"object"}'}`)
        .join(",\n          ")}
      },
      required: [],
      additionalProperties: false
    }}

    function save() {
      if (!socket || socket.readyState !== 1) {
        fetch(window.location.href, {
          method: 'POST',
          headers: {
            'Accept': 'application/json'
          },
          body: JSON.stringify(editor.get())
        })
      } else socket.send(JSON.stringify(editor.get()));
    }

    function generateSslCertificate() {
      const sslModal = new bootstrap.Modal(document.getElementById('ssl-modal'), {});
      sslModal.show()
      setTimeout(function() {
        const keypair = forge.pki.rsa.generateKeyPair(2048);
        const certificate = forge.pki.createCertificate();
        const now = new Date();
        const fiveYears = new Date(new Date(now).setFullYear(now.getFullYear() + 5));
        Object.assign(certificate, {
          publicKey: keypair.publicKey,
          serialNumber: "01",
          validity: {
            notBefore: now,
            notAfter: fiveYears,
          },
        });
        certificate.sign(keypair.privateKey, forge.md.sha256.create());
        const key = forge.pki.privateKeyToPem(keypair.privateKey);
        const cert = forge.pki.certificateToPem(certificate);
        const existingConfig = editor.get();
        editor.set({ ...existingConfig, ssl: { key, cert },
          port: parseInt(("" + existingConfig.port).replace(/(80|[0-9])80$/, '443'))
        });
        sslModal.hide();
      }, 100);
    }

    const editor = new JSONEditor(container, options);
    const initialJson = ${JSON.stringify(state.config)}
    editor.set(initialJson)
    editor.validate();
    editor.aceEditor.commands.addCommand({
      name: 'save',
      bindKey: {win: 'Ctrl-S',  mac: 'Command-S'},
      exec: save,
    });
    function startSocket() {
      if (socket != null) return;
      socket = new WebSocket("ws${state.config.ssl ? "s" : ""}://${mappingAttributes.proxyHostnameAndPort}/local-traffic-config");
      socket.onmessage = function(event) {
        editor.set(JSON.parse(event.data))
        editor.validate()
      }
      socket.onerror = function(error) {
        socket = null;
        setTimeout(startSocket, 1000);
        if (error.target.readyState === 3) {
          return;
        }
        throw new Error(\`[error] \${JSON.stringify(error)}\`);
      };
      socket.onclose = function(error) {
        socket = null;
        setTimeout(startSocket, 1000);
      };
    }
    window.addEventListener("DOMContentLoaded", function() {
      document.getElementById('jsoneditor').style.height =
        (document.documentElement.clientHeight - 150) + 'px';
      document.getElementById('jsoneditor').style.width =
        parseInt(window.getComputedStyle(
          document.querySelector('.container')).maxWidth) + 'px';
      const sslButton = document.createElement('button');
      sslButton.addEventListener("click", generateSslCertificate);
      sslButton.type="button";
      sslButton.classList.add("btn");
      sslButton.classList.add("btn-primary");
      sslButton.innerHTML="&#x1F512;";
      document.querySelector('.jsoneditor-menu')
              .appendChild(sslButton);
      const saveButton = document.createElement('button');
      saveButton.addEventListener("click", save);
      saveButton.type="button";
      saveButton.classList.add("btn");
      saveButton.classList.add("btn-primary");
      saveButton.innerHTML="&#x1F4BE;";
      document.querySelector('.jsoneditor-menu')
              .appendChild(saveButton);
      startSocket();
    });
    </script>
  </body></html>`);
};
const recorderHandler = (state, buffer, requestMethodIsDelete) => {
    let mocksUpdate = {};
    try {
        mocksUpdate = JSON.parse(buffer.toString("ascii"));
    }
    catch (e) { }
    if (typeof mocksUpdate !== "object" ||
        Object.keys(mocksUpdate).filter(key => !["strict", "mode", "mocks", "autoRecord"].includes(key)).length ||
        (!Array.isArray(mocksUpdate.mocks) && mocksUpdate.mocks !== undefined)) {
        state.log([
            [
                {
                    text: `${EMOJIS.MOCKS} invalid mocks update received`,
                    color: LogLevel.WARNING,
                },
            ],
        ]);
        return;
    }
    const { mocks: mocksArray, mode: newMode, strict: strictMode, autoRecord: autoRecordUpdate, } = mocksUpdate;
    const mocks = !mocksArray
        ? null
        : new Map(mocksArray.map(({ response, uniqueHash }) => [
            cleanEntropy(state.config, uniqueHash),
            response,
        ]));
    const modeHasBeenChangedToProxy = newMode !== state.mode && newMode === ServerMode.PROXY;
    const autoRecord = modeHasBeenChangedToProxy && autoRecordUpdate !== true
        ? false
        : autoRecordUpdate !== null && autoRecordUpdate !== void 0 ? autoRecordUpdate : state.mockConfig.autoRecord;
    const autoRecordModeHasBeenChanged = autoRecord !== undefined && autoRecord != state.mockConfig.autoRecord;
    const mocksConfigHasBeenChanged = (newMode !== state.mode && newMode === ServerMode.MOCK) ||
        (mocks !== null && state.mockConfig.mocks.size !== mocks.size);
    const strict = strictMode !== null && strictMode !== void 0 ? strictMode : state.mockConfig.strict;
    const mode = newMode !== null && newMode !== void 0 ? newMode : state.mode;
    const strictModeHasBeenChanged = !!strict !== !!state.mockConfig.strict;
    const mocksHaveBeenPurged = requestMethodIsDelete;
    if (mocksHaveBeenPurged)
        update(state, {
            mode,
            mockConfig: {
                autoRecord: false,
                strict,
                mocks: new Map(),
            },
        });
    else
        update(state, {
            mode,
            mockConfig: {
                strict,
                autoRecord,
                mocks: mocks !== null && mocks !== void 0 ? mocks : state.mockConfig.mocks,
            },
        });
    setTimeout(() => {
        var _a;
        return state.log([
            modeHasBeenChangedToProxy
                ? [
                    {
                        text: `${EMOJIS.RULES} ${Object.keys((_a = state.config.mapping) !== null && _a !== void 0 ? _a : {})
                            .length.toString()
                            .padStart(5)} loaded mapping rules`,
                        color: LogLevel.INFO,
                    },
                ]
                : null,
            mocksConfigHasBeenChanged
                ? [
                    {
                        text: `${strict ? EMOJIS.STRICT_MOCKS : EMOJIS.MOCKS} ${(mocks !== null && mocks !== void 0 ? mocks : state.mockConfig.mocks).size
                            .toString()
                            .padStart(5)} loaded mocks`,
                        color: LogLevel.INFO,
                    },
                ]
                : null,
            strictModeHasBeenChanged
                ? [
                    {
                        text: `${strict ? EMOJIS.STRICT_MOCKS : EMOJIS.MOCKS} mocks strict mode : ${strict !== null && strict !== void 0 ? strict : state.mockConfig.strict}`,
                        color: LogLevel.INFO,
                    },
                ]
                : null,
            autoRecordModeHasBeenChanged
                ? [
                    {
                        text: `${mode === ServerMode.PROXY
                            ? EMOJIS.AUTO_RECORD
                            : strict
                                ? EMOJIS.STRICT_MOCKS
                                : EMOJIS.MOCKS} mocks auto-record : ${autoRecord}`,
                        color: LogLevel.INFO,
                    },
                ]
                : null,
            modeHasBeenChangedToProxy ||
                mocksConfigHasBeenChanged ||
                autoRecordModeHasBeenChanged ||
                strictModeHasBeenChanged ||
                mocksHaveBeenPurged
                ? state.buildQuickStatus()
                : null,
        ].filter(e => e));
    }, 1);
};
exports.recorderHandler = recorderHandler;
const dataPage = (state, _request, mappingAttributes) => {
    var _a, _b, _c, _d, _e, _f;
    const [, contentType, encoding, value] = (_b = /^data:([^;,]*)?;?([^,]*)?,(.*)$/.exec((_a = mappingAttributes === null || mappingAttributes === void 0 ? void 0 : mappingAttributes.target.href) !== null && _a !== void 0 ? _a : "data:,")) !== null && _b !== void 0 ? _b : ["", "", "", ""];
    const decodedValue = decodeURIComponent(value);
    const rawText = encoding === "base64"
        ? Buffer.from(decodedValue, "base64url").toString("binary")
        : decodedValue;
    return staticResponse(!state.config.replaceResponseBodyUrls
        ? rawText
        : replaceBody(Buffer.from(rawText), {
            "content-type": contentType ? contentType : "text/plain",
        }, {
            mapping: (_c = state.config.mapping) !== null && _c !== void 0 ? _c : {},
            proxyHostnameAndPort: mappingAttributes.proxyHostnameAndPort,
            proxyHostname: (_d = mappingAttributes === null || mappingAttributes === void 0 ? void 0 : mappingAttributes.proxyHostname) !== null && _d !== void 0 ? _d : "localhost",
            key: (_e = mappingAttributes === null || mappingAttributes === void 0 ? void 0 : mappingAttributes.key) !== null && _e !== void 0 ? _e : "",
            direction: REPLACEMENT_DIRECTION.INBOUND,
            ssl: !!state.config.ssl,
            port: (_f = state.config.port) !== null && _f !== void 0 ? _f : defaultConfig === null || defaultConfig === void 0 ? void 0 : defaultConfig.port,
        }), {
        headers: { "content-type": contentType },
    });
};
const recorderPage = (state, request, mappingAttributes) => {
    var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k;
    if ((_a = request.url) === null || _a === void 0 ? void 0 : _a.endsWith("?forceLogInRecorderPage=true")) {
        return staticResponse(`{"ping":"pong"}`, {
            headers: { "content-type": "application/json; charset=utf-8" },
        });
    }
    if (request.method === "GET" &&
        ((_c = (_b = request.headers) === null || _b === void 0 ? void 0 : _b["accept"]) === null || _c === void 0 ? void 0 : _c.includes("application/json"))) {
        return staticResponse(JSON.stringify(Object.assign(Object.assign({}, state.mockConfig), { mode: state.mode, mocks: [...state.mockConfig.mocks.entries()].map(([uniqueHash, response]) => ({ uniqueHash, response })) })), {
            headers: { contentType: "application/json; charset=utf-8" },
        });
    }
    if (["PUT", "POST", "DELETE"].includes((_d = request.method) !== null && _d !== void 0 ? _d : "")) {
        return staticResponse(`{"status": "acknowledged"}`, {
            headers: { contentType: "application/json; charset=utf-8" },
            onOutboundWrite: buffer => recorderHandler(state, buffer, request.method === "DELETE"),
        });
    }
    return staticResponse(`${header(0x23fa, "recorder", "")}
<link href="${cdn}jsoneditor/dist/jsoneditor.min.css" rel="stylesheet" type="text/css">
<script src="${cdn}jsoneditor/dist/jsoneditor.min.js"></script>
<script src="${cdn}pako/dist/pako.min.js"></script>
<form>
  <div id="commands"${state.mockConfig.autoRecord ? ' style="filter:blur(8px)"' : ""}>
    <span>Mode : </span>
    <div class="btn-group" role="group" aria-label="Server Mode">
      <input type="radio" class="btn-check" name="server-mode" id="record-mode" autocomplete="off"${state.mode === ServerMode.PROXY ? " checked" : ""}>
      <label class="btn btn-outline-primary" for="record-mode">&#9210; Record</label>
      <input type="radio" class="btn-check" name="server-mode" id="mock-mode" autocomplete="off"${state.mode === ServerMode.MOCK ? " checked" : ""}>
      <label class="btn btn-outline-primary" for="mock-mode">&#x1F310; Mock</label>
    </div>
    <span>Actions : </span>
    <button type="button" class="btn btn-light" id="add-mock">&#x2795; Mock from dummy request</button>
    <button type="button" class="btn btn-light" id="upload-mocks">&#x1F4E5; Upload mocks</button>
    <button type="button" class="btn btn-light" id="download-mocks">&#x1F4E6; Download mocks</button>
    <button type="button" class="btn btn-light" id="delete-mocks">&#x1F5D1; Delete mocks</button>
  </div>
  <div class="row">
    <div class="col-lg" style="max-width: 200px">
      <div class="form-check form-switch" id="strict-mock-mode-form-control">
        <input class="form-check-input" type="checkbox" id="strict-mock-mode"${state.mockConfig.strict ? ' checked="checked"' : ""}>
        <label class="form-check-label" for="strict-mock-mode">Strict mock mode</label>
      </div>
    </div>
    <div class="col-lg" style="max-width: 200px">
      <div class="form-check form-switch">
        <input class="form-check-input" type="checkbox" id="auto-record-mode"${state.mockConfig.autoRecord ? ' checked="checked"' : ""}>
        <label class="form-check-label" for="auto-record-mode">Auto record mode</label>
      </div>
    </div>
    <div class="col-lg">&nbsp;</div>
  </div>
  <input type="hidden" id="limit" value="0"/>
  <div class="modal fade" id="edit-request" tabindex="-1" 
   aria-labelledby="edit-request-label" aria-hidden="true">
    <div class="modal-dialog" style="max-width: 900px">
      <div class="modal-content">
        <div class="modal-header">
          <h1 class="modal-title fs-5" id="edit-request-label">Edit request to /</h1>
          <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
        </div>
        <div class="modal-body">
          <div class="container">
            <div class="row">
              <div class="col-lg">
                <h2>Request :</h2>
                <div id="uniqueHash-editor" style="width: 400px; height: 400px;"></div>
              </div>
              <div class="col-lg">
                <h2>Response : </h2>
                <div id="response-editor" style="width: 400px; height: 400px;"></div>
              </div>
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
          <button type="button" class="btn btn-primary" onclick="javascript:saveRequest()">Save changes</button>
        </div>
      </div>
    </div>
  </div>
  <script>
const xmlOrJsonPrologsInBase64 = [
  "eyJ","PD94bWw=","PCFET0NUWVBF","PCFkb2N0eXBl","PGh0bWw","PEhUTUw","H4sIAAAAAAAA", "W3tc"
];
function getMocksData () {
  return JSON.stringify(
    [...document.querySelectorAll('button[data-uniqueHash]')].map(button => ({
      response: button.attributes['data-response']?.value,
      uniqueHash: button.attributes['data-uniqueHash']?.value}))
   )
}
function updateState () {
  fetch("${mappingAttributes.proxyOrigin}${(_g = (_f = Object.entries((_e = state.config.mapping) !== null && _e !== void 0 ? _e : {}).find(([_, value]) => { var _a; return (_a = value === null || value === void 0 ? void 0 : value.toString()) === null || _a === void 0 ? void 0 : _a.startsWith("recorder:"); })) === null || _f === void 0 ? void 0 : _f[0]) !== null && _g !== void 0 ? _g : "/recorder/"}", {
     method: 'PUT',
     headers: { 'Content-Type': 'application/json' },
     body: '{"strict":' + document.getElementById('strict-mock-mode').checked +
           ',"autoRecord":' + document.getElementById('auto-record-mode').checked +
           ',"mode":"' + 
           (document.getElementById('mock-mode').checked ? "mock" : "proxy") + '"' +
          ',"mocks":' + getMocksData() + '}'
   })
}
function loadMocks(mocksHashes) {
  const time = new Date().toISOString().split('T')[1].replace('Z', '');
  let mocks = [];
  try {
    mocks = mocksHashes.map(mock => ({...mock, 
      request: JSON.parse(atob(mock.uniqueHash)),
      response: JSON.parse(atob(mock.response))
    }));
  } catch(e) { }
  mocks.forEach(mock => {
    const randomId = window.crypto.randomUUID();
    const actions = getActionsHtmlText(mock.uniqueHash, mock.response);
    addNewRequest(randomId, actions, time, 'info', 'HTTP/2', mock.request.method, 
    '<span class="badge bg-' + 
        getColorFromStatusCode(mock.response.status) + '">' + 
        mock.response.status + 
        '</span>', 
        '0ms', mock.request.url, 
        'N/A');
  });
}
document.getElementById('add-mock').addEventListener('click', () => {
  const iframe = document.createElement('iframe');
  iframe.style.display = 'none';
  iframe.onload = function() { iframe.parentNode.removeChild(iframe); };
  iframe.src = "${mappingAttributes.proxyOrigin}${(_k = (_j = Object.entries((_h = state.config.mapping) !== null && _h !== void 0 ? _h : {}).find(([_, value]) => { var _a; return (_a = value === null || value === void 0 ? void 0 : value.toString()) === null || _a === void 0 ? void 0 : _a.startsWith("recorder:"); })) === null || _j === void 0 ? void 0 : _j[0]) !== null && _k !== void 0 ? _k : "/recorder/"}?forceLogInRecorderPage=true";
  document.body.appendChild(iframe);
});
document.getElementById('upload-mocks').addEventListener('click', () => {
  const time = new Date().toISOString().split('T')[1].replace('Z', '');
  const fileInput = document.createElement('input');
  fileInput.type = "file";
  fileInput.multiple = "multiple";
  fileInput.onchange = function() {
    const fileReader = new FileReader();
    [...fileInput.files].reduce((promise, file) =>
      promise.then(result => new Promise(resolve => {
        fileReader.readAsText(file);
        fileReader.onload = function(){
          resolve(result.concat(fileReader.result));
        };
      })), Promise.resolve([]))
    .then(files => files.flatMap(file => JSON.parse(file)))
    .catch(e => [])
    .then(mocks => loadMocks(mocks))
    .then(() => updateState());
  }
  fileInput.click();
});
document.getElementById('download-mocks').addEventListener('click', () => {
  const link = document.createElement('a');
  link.href = URL.createObjectURL(new Blob([getMocksData()], {
    type: "application/json",
  }));
  link.download = "mocks-" + new Date().toISOString() + ".json";
  link.click();
  URL.revokeObjectURL(link.href);
})
document.getElementById('delete-mocks').addEventListener('click', () => {
  document.getElementById('limit').value = -1;
  cleanup();
  updateState();
  document.getElementById('limit').value = 0;
})
document.getElementById('record-mode').addEventListener('change', () => {
  document.getElementById('limit').value = 0;
  cleanup();
  updateState();
})
document.getElementById('mock-mode').addEventListener('change', () => {
  updateState();
})
document.getElementById('auto-record-mode').addEventListener('change', (e) => { 
  updateState();
  document.getElementById('table-access').style.filter = 
    document.getElementById('auto-record-mode').checked ? 'blur(8px)' : 'blur(0px)';
  document.getElementById('commands').style.filter = 
      document.getElementById('auto-record-mode').checked ? 'blur(8px)' : 'blur(0px)';
  document.getElementById('alert-about-auto-record-mode').style.display = 
    document.getElementById('auto-record-mode').checked ? 'block' : 'none';
  document.getElementById('strict-mock-mode-form-control').style.filter = 
    document.getElementById('auto-record-mode').checked ? 'blur(8px)' : 'blur(0px)';
    
})
document.getElementById('strict-mock-mode').addEventListener('change', (e) => { 
  updateState();
})
function saveRequest () {
  $('#edit-request').modal("hide");
  
  const requestBeingEdited = window.requestBeingEdited;
  let request = uniqueHashEditor.get();
  let response = responseEditor.get();
  if (typeof request.body === "object") {
    request.body = JSON.stringify(request.body);
  }
  if (typeof response.body === "object") {
    response.body = JSON.stringify(response.body);
  }
  const oldRequest = JSON.parse(atob(requestBeingEdited.attributes['data-uniqueHash'].value));
  const oldResponse = JSON.parse(atob(requestBeingEdited.attributes['data-response'].value));
  const requestProlog = requestBeingEdited.attributes['data-requestProlog']?.value;
  const responseProlog = requestBeingEdited.attributes['data-responseProlog']?.value;
  const requestPrologHasChanged = request.body.substring(0, 10) !== oldRequest.body.substring(0, 10);
  const responsePrologHasChanged = response.body.substring(0, 10) !== response.body.substring(0, 10);
  if (requestProlog === "H4sIAAAAAAAA" && !requestPrologHasChanged) {
    request.body =
      btoa([...pako.gzip(request.body)].map(e => String.fromCharCode(e)).join(""));
  } else if ((requestProlog === null || !request.body.startsWith(requestProlog ?? "")) && 
      request.body.substring(0, 10) !== oldRequest.body.substring(0, 10)) {
    request.body = btoa(request.body);
  }
  if (responseProlog === "H4sIAAAAAAAA" && !responsePrologHasChanged) {
    response.body =
      btoa([...pako.gzip(response.body)].map(e => String.fromCharCode(e)).join(""));
  } else if ((responseProlog === null || !response.body.startsWith(responseProlog ?? "")) && 
      response.body.substring(0, 10) !== oldResponse.body.substring(0, 10)) {
    response.body = btoa(response.body);
  }
  request = btoa(JSON.stringify(request));
  response = btoa(JSON.stringify(response));
  requestBeingEdited.setAttribute('data-uniqueHash', request);
  requestBeingEdited.setAttribute('data-response', response);
  const row = requestBeingEdited.closest('tr');
  row.querySelector("td.method").innerHTML = uniqueHashEditor.get().method;
  row.querySelector("td.upstream-path").innerHTML = uniqueHashEditor.get().url;
  window.requestBeingEdited = undefined;
  updateState();
}
document.getElementById('edit-request').addEventListener('show.bs.modal', event => {
  const request = JSON.parse(atob(event.relatedTarget.attributes['data-uniqueHash'].value));
  const response = JSON.parse(atob(event.relatedTarget.attributes['data-response'].value));
  const requestProlog = xmlOrJsonPrologsInBase64.find(prolog => request.body?.startsWith(prolog));
  const responseProlog = xmlOrJsonPrologsInBase64.find(prolog => response.body?.startsWith(prolog));
  if (requestProlog) {
    event.relatedTarget.setAttribute('data-requestProlog', requestProlog);
    request.body = request.body.startsWith("H4sIAAAAAAAA") 
    ? pako.ungzip(new Uint8Array(atob(request.body).split("").map(e => e.charCodeAt(0))), {to: "string"})
    : atob(request.body);
    request.body = request.body.startsWith("{\\"") || request.body.startsWith("[{\\"")
      ? JSON.parse(request.body) : request.body;
  }
  if (responseProlog) {
    event.relatedTarget.setAttribute('data-responseProlog', responseProlog);
    response.body = response.body.startsWith("H4sIAAAAAAAA") 
    ? pako.ungzip(new Uint8Array(atob(response.body).split("").map(e => e.charCodeAt(0))), {to: "string"})
    : atob(response.body);
    response.body = response.body.startsWith("{\\"") || response.body.startsWith("[{\\"")
      ? JSON.parse(response.body) : response.body;
  }
  window.requestBeingEdited = event.relatedTarget;
  window.uniqueHashEditor.set(request);
  window.responseEditor.set(response);
  document.getElementById('edit-request-label').innerText = "Edit request to " + request.url;
})

setTimeout(() => {
  loadMocks(${JSON.stringify([...state.mockConfig.mocks.entries()].map(([uniqueHash, response]) => ({
        uniqueHash,
        response,
    })))});
  window.uniqueHashEditor = new JSONEditor(document.getElementById("uniqueHash-editor"), {
    mode: "code", allowSchemaSuggestions: true, schema: {
      type: "object",
      properties: {
        method: {type: "string"},
        url: {type: "string"},
        body: {oneOf: [{type:"string"},{type:"object"},{type:"array"}]},
        headers: {type: "object"},
      },
    required: [],
    additionalProperties: false
  }});
  window.responseEditor = new JSONEditor(document.getElementById("response-editor"), {
    mode: "code", allowSchemaSuggestions: true, schema: {
      type: "object",
      properties: {
        body: {oneOf: [{type:"string"},{type:"object"},{type:"array"}]},
        headers: {type: "object"},
        status: {type: "integer"}
    },
    required: [],
    additionalProperties: false
  }});
  ${state.mockConfig.autoRecord
        ? ";document.getElementById('strict-mock-mode-form-control')" +
            ".style.filter='blur(8px)';" +
            ";document.getElementById('table-access').style.filter='blur(8px)';"
        : ""}
  document.forms[0].reset();
}, 10)
</script>
</form>
<div class="alert alert-warning" role="alert"
     style="display:${state.mockConfig.autoRecord ? "block" : "none"};left:20%;right:20%;position:absolute;z-index:1;" id="alert-about-auto-record-mode">
  &#x24D8;&nbsp;Auto-record mode and recorder webapp are known to be mutually exclusive.
  <br/><br/>Changing the mocks on both sides is somehow hard to sort out.
  <br/>This is triggering concurrent modifications in the mock config.
  <hr/>
  Here is what you can do :
  <ul>
    <li>If you want to record mocks using a frontend app, turn off the auto-record mode.</li>
    <li>If you want to record mocks with the recorder API only, close this app.</li>
  </ul>
</div>
${logsView(mappingAttributes.proxyHostnameAndPort, state.config, {
        captureResponseBody: true,
    })}
</body>
</html>`);
};
const filePage = (_state, _request, mappingAttributes) => {
    const url = mappingAttributes === null || mappingAttributes === void 0 ? void 0 : mappingAttributes.target;
    const file = (0, path_1.resolve)("/", url.hostname, ...url.pathname
        .replace(/[?#].*$/, "")
        .replace(/^\/+/, "")
        .split("/")
        .map(decodeURIComponent));
    return {
        alpnProtocol: "file",
        error: null,
        data: null,
        hasRun: false,
        run: function () {
            return this.hasRun
                ? Promise.resolve()
                : new Promise(promiseResolve => (0, fs_1.readFile)(file, (error, data) => {
                    this.hasRun = true;
                    if (!error || error.code !== "EISDIR") {
                        this.error = error;
                        this.data = data;
                        promiseResolve(void 0);
                        return;
                    }
                    (0, fs_1.readdir)(file, (readDirError, filelist) => {
                        this.error = readDirError;
                        this.data = filelist;
                        if (readDirError) {
                            promiseResolve(void 0);
                            return;
                        }
                        Promise.all(filelist.map(file => new Promise(innerResolve => (0, fs_1.lstat)((0, path_1.resolve)(url.pathname, file), (err, stats) => innerResolve([file, stats, err]))))).then(filesWithTypes => {
                            const entries = filesWithTypes
                                .filter(entry => !entry[2] && entry[1].isDirectory())
                                .concat(filesWithTypes.filter(entry => !entry[2] && entry[1].isFile()));
                            this.data = `${header(0x1f4c2, "directory", url.href)}<p>Directory content of <i>${url.href.replace(/\//g, "&#x002F;")}</i></p><ul class="list-group"><li class="list-group-item">&#x1F4C1;<a href="${url.pathname.endsWith("/") ? ".." : "."}">&lt;parent&gt;</a></li>${entries
                                .filter(entry => !entry[2])
                                .map(entry => {
                                const type = entry[1].isDirectory() ? 0x1f4c1 : 0x1f4c4;
                                return `<li class="list-group-item">&#x${type.toString(16)};<a href="${url.pathname.endsWith("/")
                                    ? ""
                                    : `${url.pathname.split("/").slice(-1)[0]}/`}${entry[0]}">${entry[0]}</a></li>`;
                            })
                                .join("\n")}</li></ul></body></html>`;
                            promiseResolve(void 0);
                        });
                    });
                }));
        },
        events: {},
        on: function (name, action) {
            this.events[name] = action;
            this.run().then(() => {
                if (name === "response")
                    this.events["response"](file.endsWith(".svg")
                        ? {
                            Server: "local",
                            "Content-Type": "image/svg+xml",
                        }
                        : file.endsWith(".js") || file.endsWith(".jsx")
                            ? {
                                Server: "local",
                                "Content-Type": "application/javascript",
                            }
                            : { Server: "local" }, 0);
                if (name === "data" && this.data) {
                    this.events["data"](this.data);
                    this.events["end"]();
                }
                if (name === "error" && this.error) {
                    this.events["error"](this.error);
                }
            });
            return this;
        },
        end: function () {
            return this;
        },
        request: function () {
            return this;
        },
        write: function () {
            return this;
        },
    };
};
const http2Page = (state, mappingAttributes) => __awaiter(void 0, void 0, void 0, function* () {
    var _a, _b;
    let error = null;
    let http2IsSupported = ((_a = mappingAttributes === null || mappingAttributes === void 0 ? void 0 : mappingAttributes.target) === null || _a === void 0 ? void 0 : _a.protocol) === "https:" &&
        !state.config.dontUseHttp2Downstream;
    const http2Connection = !http2IsSupported
        ? null
        : state.mode !== ServerMode.PROXY && ((_b = state === null || state === void 0 ? void 0 : state.mockConfig) === null || _b === void 0 ? void 0 : _b.strict)
            ? null
            : yield Promise.race([
                new Promise(resolve => {
                    const result = (0, http2_1.connect)(mappingAttributes.target, {
                        timeout: state.config.connectTimeout,
                        sessionTimeout: state.config.socketTimeout,
                        rejectUnauthorized: false,
                        protocol: mappingAttributes.target.protocol,
                    }, (_, socketPath) => {
                        http2IsSupported =
                            http2IsSupported && !!socketPath.alpnProtocol;
                        resolve(!http2IsSupported ? null : result);
                    });
                    result.on("error", (thrown) => {
                        error = !http2IsSupported
                            ? null
                            : Buffer.from(errorPage(thrown, state.mode, "connection", mappingAttributes.url, mappingAttributes.target));
                    });
                }),
                new Promise(resolve => setTimeout(() => {
                    http2IsSupported = false;
                    resolve(null);
                }, state.config.connectTimeout)),
            ]);
    if (error)
        throw error;
    return http2IsSupported ? http2Connection : null;
});
const http1Page = (target, url, targetUrl, fullPath, inboundRequest, outboundHeaders, requestBody, bufferedRequestBody, mode) => __awaiter(void 0, void 0, void 0, function* () {
    const http1RequestOptions = {
        hostname: target.hostname,
        path: fullPath,
        port: target.port ? target.port : target.protocol === "https:" ? 443 : 80,
        protocol: target.protocol,
        rejectUnauthorized: false,
        method: inboundRequest.method,
        headers: Object.assign(Object.assign({}, Object.assign({}, ...Object.entries(outboundHeaders)
            .filter(([h]) => !h.startsWith(":") && h.toLowerCase() !== "transfer-encoding")
            .map(([key, value]) => ({ [key]: value })))), { host: target.hostname }),
    };
    let error = null;
    const outboundHttp1Response = error
        ? null
        : [...specialProtocols].includes(target.protocol)
            ? null
            : yield new Promise(resolve => {
                const outboundHttp1Request = target.protocol === "https:"
                    ? (0, https_1.request)(http1RequestOptions, resolve)
                    : (0, http_1.request)(http1RequestOptions, resolve);
                outboundHttp1Request.on("error", thrown => {
                    error = Buffer.from(errorPage(thrown, mode, "request", url, targetUrl));
                    resolve(null);
                });
                if (bufferedRequestBody) {
                    outboundHttp1Request.write(requestBody);
                    outboundHttp1Request.end();
                }
                if (!bufferedRequestBody) {
                    inboundRequest.on("data", chunk => outboundHttp1Request.write(chunk));
                    inboundRequest.on("end", () => outboundHttp1Request.end());
                }
            });
    if (error)
        throw error;
    return {
        alpnProtocol: "HTTP1.1",
        error: null,
        data: null,
        hasRun: false,
        events: {},
        on: function (name, action) {
            if (name === "response")
                return action === null || action === void 0 ? void 0 : action(Object.assign(Object.assign({}, outboundHttp1Response.headers), { [":status"]: outboundHttp1Response.statusCode, [":statusmessage"]: outboundHttp1Response.statusMessage }));
            return outboundHttp1Response.on(name, action);
        },
        end: function () {
            return this;
        },
        close: function () {
            outboundHttp1Response.destroy();
            return this;
        },
        request: function () {
            return this;
        },
        write: function () {
            return this;
        },
    };
});
const workerPage = (state, _, mappingAttributes) => {
    return staticResponse(`
const mapping = ${JSON.stringify(Object.entries(state.config.mapping)
        .filter(key => key && key.length > 1)
        .map(([key, value]) => {
        var _a, _b;
        if (typeof value === "string" && value.startsWith("data:"))
            return [key, "data:text/plain,..."];
        let match = (_a = key.match(RegExp(key.replace(/^\//, "^/")))) !== null && _a !== void 0 ? _a : null;
        const replacedReplaceBody = (_b = (typeof value === "string" ? value : value.replaceBody)) === null || _b === void 0 ? void 0 : _b.replace(/\$\$(\d+)/g, (_, index) => { var _a; return (_a = match === null || match === void 0 ? void 0 : match[parseInt(index)]) !== null && _a !== void 0 ? _a : ""; });
        let replacementCounter = 0;
        return [
            key.replace(/\([^)]+\)/g, () => `$${++replacementCounter}`),
            replacedReplaceBody,
        ];
    }))}
self.addEventListener("install", function () {
  self.skipWaiting();
});
self.addEventListener("activate", function (event) {
  event.waitUntil(self.clients.claim());
});
self.addEventListener("fetch", function (event) {
  let canonicalUrl = '';
  try {
    canonicalUrl = new URL(event.request.url);
  } catch(e) {}
  if (!canonicalUrl || canonicalUrl.hostname === "${mappingAttributes.proxyHostnameAndPort}") return;
  const resolvedUrl = mapping.reduce((url, [to, from]) => 
    url.replace(new RegExp(from, "ig"), to), event.request.url);
  if (resolvedUrl === event.request.url) return;
  event.respondWith(fetch(new URL(resolvedUrl, "${mappingAttributes.proxyOrigin}").href),{
      method: event.request.method, 
      headers: event.request.headers,
      body: event.request.body,
      mode: event.request.mode,
      credentials: event.request.credentials,
      cache: event.request.cache,
      redirect: event.request.redirect,
      referrer: event.request.referrer,
      referrerPolicy: event.request.referrerPolicy,
      integrity: event.request.integrity,
      keepalive: event.request.keepalive,
      signal: event.request.signal,
      destination: event.request.destination,
})
});`, {
        headers: {
            "content-type": "text/javascript; charset=utf8",
            "Service-Worker-Allowed": "/",
        },
    });
};
const specialPageMapping = {
    logs: logsPage,
    config: configPage,
    recorder: recorderPage,
    file: filePage,
    data: dataPage,
    worker: workerPage,
};
const specialPages = Object.keys(specialPageMapping);
const specialProtocols = specialPages.map(page => `${page}:`);
const defaultConfig = {
    mapping: Object.assign({}, ...specialPages
        .filter(page => page !== "data" && page !== "file")
        .map(page => ({
        [page === "worker" ? "/local-traffic-worker.js" : `/${page}/`]: `${page}://`,
    }))),
    port: 8080,
    replaceRequestBodyUrls: false,
    replaceResponseBodyUrls: false,
    dontUseHttp2Downstream: false,
    dontTranslateLocationHeader: false,
    logAccessInTerminal: false,
    simpleLogs: false,
    websocket: true,
    disableWebSecurity: false,
    connectTimeout: 3000,
    socketTimeout: 3000,
    unwantedHeaderNamesInMocks: [],
};
const load = (...args_1) => __awaiter(void 0, [...args_1], void 0, function* (firstTime = true) {
    return new Promise(resolve => (0, fs_1.readFile)(filename, (error, data) => {
        if (error) {
            log(null, [
                [
                    {
                        text: `${EMOJIS.ERROR_1} config error. Using previous value`,
                        color: LogLevel.ERROR,
                    },
                ],
            ]).then(() => resolve(Object.assign({}, defaultConfig)));
            if (error.code !== "ENOENT")
                return;
        }
        let config = null;
        try {
            config = Object.assign({}, defaultConfig, JSON.parse((data !== null && data !== void 0 ? data : "{}").toString()));
        }
        catch (e) {
            config = config !== null && config !== void 0 ? config : Object.assign({}, defaultConfig);
            return log({ config: undefined }, [
                [
                    {
                        text: `${EMOJIS.ERROR_2} config syntax incorrect, ignoring`,
                        color: LogLevel.ERROR,
                    },
                ],
            ]).then(() => resolve(config));
        }
        if ((error === null || error === void 0 ? void 0 : error.code) === "ENOENT" && firstTime) {
            (0, fs_1.writeFile)(filename, JSON.stringify(defaultConfig, null, 2), fileWriteErr => {
                return (fileWriteErr
                    ? log(null, [
                        [
                            {
                                text: `${EMOJIS.ERROR_4} config file NOT created`,
                                color: LogLevel.ERROR,
                            },
                        ],
                    ])
                    : log(null, [
                        [
                            {
                                text: `${EMOJIS.COLORED} config file created`,
                                color: LogLevel.INFO,
                            },
                        ],
                    ])).then(() => resolve(config));
            });
        }
        else
            resolve(config);
    })).then((readConfig) => __awaiter(void 0, void 0, void 0, function* () {
        return !readConfig
            ? readConfig
            : yield Promise.all(Object.entries(readConfig.mapping).map(([key, mapping]) => new Promise(resolve => {
                const replaceBody = typeof mapping === "string" ? null : mapping.replaceBody;
                const downstreamUrl = typeof mapping === "string" ? mapping : mapping.downstreamUrl;
                if (!downstreamUrl.startsWith("file:") || key.endsWith("(.*)"))
                    return resolve([key, mapping]);
                const matchersCount = downstreamUrl
                    .split("")
                    .map((c, i) => c + downstreamUrl.charAt(i + 1))
                    .filter(m => m === "$$").length;
                return (0, fs_1.lstat)(new url_1.URL(downstreamUrl.replace(/\$\$[0-9]+/g, "")).pathname, (e, stats) => {
                    if (e)
                        return resolve([key, mapping]);
                    const isDirectory = stats.isDirectory();
                    const replacedKey = isDirectory
                        ? `${key === null || key === void 0 ? void 0 : key.replace(/\/*$/g, "")}/(.*)`
                        : key;
                    const replacedReplaceBody = isDirectory
                        ? `${replaceBody === null || replaceBody === void 0 ? void 0 : replaceBody.replace(/\/*$/g, "")}/$$${matchersCount + 1}`
                        : replaceBody;
                    const replacedDownstreamUrl = isDirectory
                        ? `${downstreamUrl.replace(/\/*$/g, "")}${path_1.sep}$$${matchersCount + 1}`
                        : downstreamUrl;
                    return resolve(!replaceBody
                        ? [replacedKey, replacedDownstreamUrl]
                        : [
                            replacedKey,
                            {
                                replaceBody: replacedReplaceBody,
                                downstreamUrl: replacedDownstreamUrl,
                            },
                        ]);
                });
            }))).then(interpretedMapping => (Object.assign(Object.assign({}, readConfig), { mapping: Object.fromEntries(interpretedMapping) })));
    }));
});
exports.load = load;
const onWatch = function (state) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a, _b, _c, _d, _e;
        const previousConfig = state.config;
        const config = yield load(false);
        const logElements = [];
        if (!config)
            return {};
        if (isNaN((_a = config === null || config === void 0 ? void 0 : config.port) !== null && _a !== void 0 ? _a : NaN) ||
            ((_b = config === null || config === void 0 ? void 0 : config.port) !== null && _b !== void 0 ? _b : -1) > 65535 ||
            ((_c = config === null || config === void 0 ? void 0 : config.port) !== null && _c !== void 0 ? _c : -1) < 0) {
            yield state.log([
                [
                    {
                        text: `${EMOJIS.PORT} port number invalid. Not refreshing`,
                        color: LogLevel.ERROR,
                    },
                ],
            ]);
            return {};
        }
        if (!((_d = config === null || config === void 0 ? void 0 : config.mapping) === null || _d === void 0 ? void 0 : _d[""])) {
            logElements.push({
                text: `${EMOJIS.ERROR_3} default mapping "" not provided.`,
                color: LogLevel.WARNING,
            });
        }
        if (typeof config.mapping !== "object") {
            state.log([
                [
                    {
                        text: `${EMOJIS.ERROR_5} mapping should be an object. Aborting`,
                        color: LogLevel.ERROR,
                    },
                ],
            ]);
            return {};
        }
        if (config.replaceRequestBodyUrls !== previousConfig.replaceRequestBodyUrls) {
            logElements.push({
                text: `${EMOJIS.REWRITE} request body url ${!config.replaceRequestBodyUrls ? "NO " : ""}rewriting`,
                color: LogLevel.INFO,
            });
        }
        if (config.replaceResponseBodyUrls !== previousConfig.replaceResponseBodyUrls) {
            logElements.push({
                text: `${EMOJIS.REWRITE} response body url ${!config.replaceResponseBodyUrls ? "NO " : ""}rewriting`,
                color: LogLevel.INFO,
            });
        }
        if (config.dontTranslateLocationHeader !==
            previousConfig.dontTranslateLocationHeader) {
            logElements.push({
                text: `${EMOJIS.REWRITE} response location header ${config.dontTranslateLocationHeader ? "NO " : ""}translation`,
                color: LogLevel.INFO,
            });
        }
        if (config.dontUseHttp2Downstream !== previousConfig.dontUseHttp2Downstream) {
            logElements.push({
                text: `${EMOJIS.OUTBOUND} http/2 ${config.dontUseHttp2Downstream ? "de" : ""}activated downstream`,
                color: LogLevel.INFO,
            });
        }
        if (config.disableWebSecurity !== previousConfig.disableWebSecurity) {
            logElements.push({
                text: `${EMOJIS.SHIELD} web security ${config.disableWebSecurity ? "de" : ""}activated`,
                color: LogLevel.INFO,
            });
        }
        if (config.websocket !== previousConfig.websocket) {
            logElements.push({
                text: `${EMOJIS.WEBSOCKET} websocket ${!config.websocket ? "de" : ""}activated`,
                color: LogLevel.INFO,
            });
        }
        if (config.logAccessInTerminal !== previousConfig.logAccessInTerminal) {
            logElements.push({
                text: `${EMOJIS.LOGS} access terminal logging ${config.logAccessInTerminal === true
                    ? "on"
                    : config.logAccessInTerminal === "with-mapping"
                        ? ": show both path and mapping"
                        : "off"}`,
                color: LogLevel.INFO,
            });
        }
        if (config.simpleLogs !== previousConfig.simpleLogs) {
            logElements.push({
                text: `${EMOJIS.COLORED} simple logs ${!config.simpleLogs ? "off" : "on"}`,
                color: LogLevel.INFO,
            });
        }
        if (Object.keys(config.mapping).join("\n") !==
            Object.keys((_e = previousConfig.mapping) !== null && _e !== void 0 ? _e : {}).join("\n")) {
            logElements.push({
                text: `${EMOJIS.RULES} ${Object.keys(config.mapping)
                    .length.toString()
                    .padStart(5)} loaded mapping rules`,
                color: LogLevel.INFO,
            });
        }
        if (config.port !== previousConfig.port) {
            logElements.push({
                text: `${EMOJIS.PORT} port changed from ${previousConfig.port} to ${config.port}`,
                color: LogLevel.INFO,
            });
        }
        if (config.ssl && !previousConfig.ssl) {
            logElements.push({
                text: `${EMOJIS.INBOUND} ssl configuration added`,
                color: LogLevel.INFO,
            });
        }
        if (!config.ssl && previousConfig.ssl) {
            logElements.push({
                text: `${EMOJIS.INBOUND} ssl configuration removed`,
                color: LogLevel.INFO,
            });
        }
        const shouldRestartServer = config.port !== previousConfig.port ||
            JSON.stringify(config.ssl) !== JSON.stringify(previousConfig.ssl);
        if (shouldRestartServer) {
            logElements.push({
                text: `${EMOJIS.RESTART} restarting server`,
                color: LogLevel.INFO,
            });
        }
        setTimeout(() => {
            quickStatus.apply(Object.assign(Object.assign({}, state), { config }), [logElements.map(line => [line])]);
        }, 1);
        return { config, server: shouldRestartServer ? null : undefined };
    });
};
const unixNorm = (path) => path == "" ? "" : (0, path_1.normalize)(path).replace(/\\/g, "/")
    .replace(/^\.\//, "");
const cdn = "https://cdn.jsdelivr.net/npm/";
const disallowedHttp2HeaderNames = [
    "host",
    "connection",
    "keep-alive",
    "upgrade",
    "transfer-encoding",
    "upgrade-insecure-requests",
    "proxy-connection",
];
const header = (icon, category, pageTitle) => `<!doctype html>
<html lang="en">
<head>
<title>&#x${icon.toString(16)}; local-traffic ${category} | ${pageTitle}</title>
<link href="${cdn}bootstrap/dist/css/bootstrap.min.css" rel="stylesheet"/>
<script src="${cdn}jquery/dist/jquery.min.js"></script>
<script src="${cdn}bootstrap/dist/js/bootstrap.bundle.min.js"></script>
</head>
<body><div class="container"><h1>&#x${icon.toString(16)}; local-traffic ${category}</h1>
<br/>`;
const mockRequest = ({ response, }) => {
    return {
        alpnProtocol: "mock",
        error: null,
        data: null,
        hasRun: false,
        run: function () {
            return this.hasRun
                ? Promise.resolve()
                : new Promise(promiseResolve => {
                    try {
                        this.data = JSON.parse(Buffer.from(response, "base64").toString("utf-8"));
                    }
                    catch (e) {
                        this.data = {};
                    }
                    promiseResolve(void 0);
                });
        },
        events: {},
        on: function (name, action) {
            this.events[name] = action;
            this.run().then(() => {
                var _a;
                if (name === "response")
                    this.events["response"](Object.assign(Object.assign({}, this.data.headers), { "X-LocalTraffic-Mock": "1" }), this.data.status);
                if (name === "data" && this.data) {
                    this.events["data"](Buffer.from((_a = this.data.body) !== null && _a !== void 0 ? _a : "", "base64"));
                    this.events["end"]();
                }
                if (name === "error" && this.error) {
                    this.events["error"](this.error);
                }
            });
            return this;
        },
        end: function () {
            return this;
        },
        request: function () {
            return this;
        },
        write: function () {
            return this;
        },
    };
};
const staticResponse = (data, options) => ({
    alpnProtocol: "static",
    error: null,
    data: null,
    outboundData: null,
    run: function () {
        return typeof data === "string"
            ? new Promise(resolve => {
                this.data = data;
                resolve(void 0);
            })
            : data.then(text => {
                this.data = text.toString("utf8");
            });
    },
    events: {},
    on: function (name, action) {
        this.events[name] = action;
        this.run().then(() => {
            var _a;
            if (name === "response")
                this.events["response"](Object.assign({ Server: "local", "Content-Type": "text/html" }, ((_a = options === null || options === void 0 ? void 0 : options.headers) !== null && _a !== void 0 ? _a : {})), 0);
            if (name === "data" && this.data) {
                this.events["data"](this.data);
                this.events["end"]();
            }
            if (name === "error" && this.error) {
                this.events["error"](this.error);
            }
        });
        return this;
    },
    write: function (payload) {
        var _a;
        this.outboundData = payload;
        if (payload instanceof Buffer)
            (_a = options === null || options === void 0 ? void 0 : options.onOutboundWrite) === null || _a === void 0 ? void 0 : _a.call(options, payload);
        return this;
    },
    end: function () {
        return this;
    },
    request: function () {
        return this;
    },
});
const replaceBody = (payloadBuffer, headers, parameters) => __awaiter(void 0, void 0, void 0, function* () {
    var _a, _b;
    return ((_b = (_a = headers["content-encoding"]) === null || _a === void 0 ? void 0 : _a.toString()) !== null && _b !== void 0 ? _b : "")
        .split(",")
        .reduce((buffer, formatNotTrimed) => __awaiter(void 0, void 0, void 0, function* () {
        const format = formatNotTrimed.trim().toLowerCase();
        const method = (format === "gzip" || format === "x-gzip"
            ? zlib_1.gunzip
            : format === "deflate"
                ? zlib_1.inflate
                : format === "br"
                    ? zlib_1.brotliDecompress
                    : format === "identity" || format === ""
                        ? (input, callback) => {
                            callback(null, input);
                        }
                        : null);
        if (method === null) {
            throw new Error(`${format} compression not supported by the proxy`);
        }
        const openedBuffer = yield buffer;
        return yield new Promise((resolve, reject) => method(openedBuffer, (err_1, data_1) => {
            if (err_1) {
                reject(err_1);
            }
            resolve(data_1);
        }));
    }), Promise.resolve(payloadBuffer))
        .then((uncompressedBuffer) => {
        const fileTooBig = uncompressedBuffer.length > 1e7;
        const fileHasSpecialChars = () => /[^\x00-\xFF]/.test(uncompressedBuffer.toString());
        const contentTypeCanBeProcessed = [
            "text/html",
            "application/javascript",
            "application/json",
        ].some(allowedContentType => { var _a; return ((_a = headers["content-type"]) !== null && _a !== void 0 ? _a : "").toString().includes(allowedContentType); });
        const willReplace = !fileTooBig && (contentTypeCanBeProcessed || !fileHasSpecialChars());
        const workerRoute = Object.entries(parameters.mapping).filter(([_, v]) => { var _a; return (_a = v === null || v === void 0 ? void 0 : v.toString()) === null || _a === void 0 ? void 0 : _a.startsWith("worker://"); })[0];
        return !willReplace
            ? uncompressedBuffer
            : replaceTextUsingMapping(uncompressedBuffer.toString(), {
                direction: parameters.direction,
                proxyHostnameAndPort: parameters.proxyHostnameAndPort,
                ssl: parameters.ssl,
                mapping: parameters.mapping,
            })
                .replace(/\?protocol=wss?%3A&hostname=[^&]+&port=[0-9]+&pathname=/g, `?protocol=ws${parameters.ssl ? "s" : ""}%3A&hostname=${parameters.proxyHostname}&port=${parameters.port}&pathname=${encodeURIComponent(parameters.key.replace(/\/{1,6}$/, ""))}`)
                .replace(/<\/head>/, () => {
                var _a;
                if (parameters.direction !== REPLACEMENT_DIRECTION.INBOUND ||
                    !((_a = headers["content-type"]) !== null && _a !== void 0 ? _a : "")
                        .toString()
                        .includes("text/html") ||
                    !workerRoute)
                    return "</head>";
                return `<script type="text/javascript">navigator.serviceWorker.register("${workerRoute[0]}",{scope:"/"});</script></head>`;
            });
    })
        .then((updatedBody) => {
        var _a, _b;
        return ((_b = (_a = headers["content-encoding"]) === null || _a === void 0 ? void 0 : _a.toString()) !== null && _b !== void 0 ? _b : "")
            .split(",")
            .reverse()
            .reduce((buffer, formatNotTrimed) => {
            const format = formatNotTrimed.trim().toLowerCase();
            const method = (format === "gzip" || format === "x-gzip"
                ? zlib_1.gzip
                : format === "deflate"
                    ? zlib_1.deflate
                    : format === "br"
                        ? zlib_1.brotliCompress
                        : format === "identity" || format === ""
                            ? (input, callback) => {
                                callback(null, input);
                            }
                            : null);
            if (method === null)
                throw new Error(`${format} compression not supported by the proxy`);
            return buffer.then(data => new Promise(resolve => method(data, (err, data) => {
                if (err)
                    throw err;
                resolve(data);
            })));
        }, Promise.resolve(typeof updatedBody === "string"
            ? Buffer.from(updatedBody)
            : updatedBody));
    });
});
exports.replaceBody = replaceBody;
const replaceTextUsingMapping = (text, { direction, proxyHostnameAndPort, ssl, mapping, }) => Object.entries(mapping)
    .map(([key, value]) => [
    key,
    typeof value === "string" ? value : value.replaceBody,
])
    .reduce((inProgress, [path, value]) => {
    const pathRegexes = path
        .split("")
        .filter(e => ["(", ")"].includes(e))
        .join("")
        .match(/^(\(\))*$/)
        ? path
            .split("(")
            .flatMap(e => e.split(")"))
            .filter((_, i) => i % 2 === 1)
        : [];
    let replacementCounter = 0;
    return specialProtocols.some(protocol => value.startsWith(protocol)) ||
        (path !== "" &&
            !(path.match(/^[-a-zA-Z0-9()@:%_\+.~#?&//=]*$/) || pathRegexes.length))
        ? inProgress
        : direction === REPLACEMENT_DIRECTION.INBOUND
            ? inProgress.replace(new RegExp(value
                .replace(new RegExp(`^(${specialPages.join("|")}):\/\/`), "")
                .replace(/\$\$(\d+)/g, (_, index) => { var _a; return "(" + ((_a = pathRegexes[parseInt(index) - 1]) !== null && _a !== void 0 ? _a : "") + ")"; })
                .replace(/\.\*/g, "[-a-zA-Z0-9()@:%_+.~#?&//=]*")
                .replace(pathRegexes.length ? "" : /[*+?^${}()|[\]\\]/g, "")
                .replace(/^https/, "https?") +
                (pathRegexes.length ? "" : "/*"), "ig"), `http${ssl ? "s" : ""}://${proxyHostnameAndPort}${path
                .replace(/\([^)]+\)/g, () => `$${++replacementCounter}`)
                .replace(/\/+$/, "/")
                .replace(/^(?![^/])$/, "/")}`)
            : inProgress
                .split(`http${ssl ? "s" : ""}://${proxyHostnameAndPort}${path
                .replace(/\/+$/, "")
                .replace(/^(?![^/])$/, "/")}`)
                .join(value);
}, text)
    .split(`${proxyHostnameAndPort}/:`)
    .join(`${proxyHostnameAndPort}:`);
exports.replaceTextUsingMapping = replaceTextUsingMapping;
const cleanEntropy = (config, requestObject) => {
    try {
        const request = typeof requestObject === "object"
            ? requestObject
            : JSON.parse(Buffer.from(requestObject, "base64").toString("utf-8"));
        [
            "access-control-max-age",
            "authorization",
            "cache-control",
            "cookie",
            "date",
            "dnt",
            "expires",
            "if-modified-since",
            "if-unmodified-since",
            "keep-alive",
            "last-modified",
            // cache header not helpful here
            "pragma",
            "proxy-authenticate",
            "proxy-authorization",
            "referer",
            // referer is more of a nuisance than a true discriminant
            "retry-after",
            "signed-headers",
            "server-timing",
            "sec-ch-ua",
            "sec-ch-ua-mobile",
            "sec-ch-ua-platform",
            "sec-fetch-dest",
            // disable check on action triggering the request
            "sec-fetch-mode",
            // disable check on action triggering the request
            "sec-fetch-site",
            // disable check on same-origin domain
            "sec-fetch-user",
            // disable check on persona triggering the request
            "upgrade-insecure-requests",
            // disable unwanted security check
            "user-agent",
            // no user agent comparison
            ...(Array.isArray(config.unwantedHeaderNamesInMocks)
                ? config.unwantedHeaderNamesInMocks
                : []),
        ].forEach(header => {
            var _a;
            (_a = request === null || request === void 0 ? void 0 : request.headers) === null || _a === void 0 ? true : delete _a[header];
        });
        request.headers = Object.keys(request.headers)
            .sort()
            .reduce((obj, key) => {
            obj[key] = request.headers[key];
            return obj;
        }, {});
        return Buffer.from(JSON.stringify(request), "utf-8").toString("base64");
    }
    catch (e) {
        // this cannot fail when a request object is passed as parameter
        return requestObject;
    }
};
exports.cleanEntropy = cleanEntropy;
const send = (code, inboundResponse, errorBuffer) => {
    inboundResponse.writeHead(code, {
        "content-type": "text/html",
        "content-length": errorBuffer.length,
    });
    inboundResponse.end(errorBuffer);
};
exports.send = send;
const determineMapping = (inboundRequest, parameters) => {
    var _a, _b, _c, _d, _e, _f, _g, _h;
    const proxyHostname = ((_c = (_b = (_a = inboundRequest.headers[":authority"]) === null || _a === void 0 ? void 0 : _a.toString()) !== null && _b !== void 0 ? _b : inboundRequest.headers.host) !== null && _c !== void 0 ? _c : "localhost").replace(/:.*/, "");
    const proxyHostnameAndPort = inboundRequest.headers[":authority"] ||
        `${inboundRequest.headers.host}${((_d = inboundRequest.headers.host) !== null && _d !== void 0 ? _d : "").match(/:[0-9]+$/)
            ? ""
            : parameters.port === 80 && !parameters.ssl
                ? ""
                : parameters.port === 443 && parameters.ssl
                    ? ""
                    : `:${(_e = parameters.port) !== null && _e !== void 0 ? _e : 8080}`}`;
    const url = new url_1.URL(`http${parameters.ssl ? "s" : ""}://${proxyHostnameAndPort}${(_f = inboundRequest.url) !== null && _f !== void 0 ? _f : ""}`);
    const path = url.href.substring(url.origin.length);
    const mappings = Object.assign({}, Object.assign({}, ...Object.entries((_g = parameters.mapping) !== null && _g !== void 0 ? _g : {}).map(([key, entry]) => {
        var _a, _b, _c;
        const value = typeof entry === "string" ? entry : (_a = entry === null || entry === void 0 ? void 0 : entry.downstreamUrl) !== null && _a !== void 0 ? _a : "";
        const matchedKey = typeof entry === "string" ? key : (_b = entry === null || entry === void 0 ? void 0 : entry.replaceBody) !== null && _b !== void 0 ? _b : "";
        let url = null;
        try {
            url = new url_1.URL(((_c = value === null || value === void 0 ? void 0 : value.startsWith) === null || _c === void 0 ? void 0 : _c.call(value, "data:")) ? value : unixNorm(value));
        }
        catch (e) { }
        return {
            [matchedKey]: url,
            [key]: url,
        };
    })));
    const matchedElements = Object.entries(mappings).filter(([key]) => { var _a; return (_a = path.match(RegExp(key.replace(/^\//, "^/")))) !== null && _a !== void 0 ? _a : null; });
    const [key, rawTarget] = (_h = matchedElements.find(matchedElement => Object.values(matchedElement).every(v => v !== null))) !== null && _h !== void 0 ? _h : ["/"];
    const match = path.match(RegExp(key.replace(/^\//, "^/")));
    const missingMappings = matchedElements
        .filter(e => Object.values(e).some(v => v === null))
        .map(([k]) => k);
    if (missingMappings.length) {
        setTimeout(() => log({}, [
            [
                {
                    text: `${EMOJIS.ERROR_4} mapping error for key(s) ${missingMappings}`,
                    color: LogLevel.WARNING,
                },
            ],
        ]), 1);
    }
    const target = !match || !rawTarget
        ? null
        : new url_1.URL(rawTarget.href.replace(/\$\$(\d+)/g, (_, index) => { var _a; return (_a = match[parseInt(index)]) !== null && _a !== void 0 ? _a : ""; }));
    return { proxyHostname, proxyHostnameAndPort, url, path, key, target };
};
exports.determineMapping = determineMapping;
const websocketServe = function (state, request, upstreamSocket) {
    var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k;
    upstreamSocket.on("error", () => {
        state.log([
            [
                {
                    text: `${EMOJIS.WEBSOCKET} websocket connection reset`,
                    color: LogLevel.WARNING,
                },
            ],
        ]);
    });
    if (!state.config.websocket) {
        upstreamSocket.end(`HTTP/1.1 503 Service Unavailable\r\n\r\n`);
        return {};
    }
    const { key, target: targetWithForcedPrefix, path, url, } = determineMapping(request, state.config);
    if (path.startsWith("/local-traffic-logs")) {
        acknowledgeWebsocket(upstreamSocket, (_a = request.headers["sec-websocket-key"]) !== null && _a !== void 0 ? _a : "");
        return {
            logsListeners: state.logsListeners.concat({
                stream: upstreamSocket,
                wantsMask: !((_c = (_b = request.headers["user-agent"]) === null || _b === void 0 ? void 0 : _b.toString()) !== null && _c !== void 0 ? _c : "").includes("Chrome"),
                wantsResponseMessage: [...url.searchParams.entries()].some(([key, value]) => key === "wantsResponseMessage" && value === "true"),
            }),
        };
    }
    if (path === "/local-traffic-config") {
        acknowledgeWebsocket(upstreamSocket, (_d = request.headers["sec-websocket-key"]) !== null && _d !== void 0 ? _d : "");
        let partialRead = null;
        upstreamSocket.on("data", buffer => {
            const read = readWebsocketBuffer(buffer, partialRead);
            if (partialRead === null && read.body.length < read.payloadLength) {
                partialRead = read;
            }
            else if (read.body.length >= read.payloadLength &&
                read.body.length === 0) {
                return {};
            }
            else if (read.body.length >= read.payloadLength) {
                partialRead = null;
                let newConfig;
                try {
                    newConfig = JSON.parse(read.body);
                }
                catch (e) {
                    state.log([
                        [
                            {
                                text: `${EMOJIS.ERROR_4} config file NOT read, try again later`,
                                color: LogLevel.WARNING,
                            },
                        ],
                    ]);
                    return {};
                }
                update(state, { pendingConfigSave: newConfig });
            }
        });
        return {
            configListeners: state.configListeners.concat({
                stream: upstreamSocket,
                wantsMask: !((_f = (_e = request.headers["user-agent"]) === null || _e === void 0 ? void 0 : _e.toString()) !== null && _f !== void 0 ? _f : "").includes("Chrome"),
            }),
        };
    }
    const target = new url_1.URL(`${(_g = targetWithForcedPrefix === null || targetWithForcedPrefix === void 0 ? void 0 : targetWithForcedPrefix.protocol) !== null && _g !== void 0 ? _g : "https"}//${(_h = targetWithForcedPrefix === null || targetWithForcedPrefix === void 0 ? void 0 : targetWithForcedPrefix.host) !== null && _h !== void 0 ? _h : "localhost"}${(_k = (_j = request.url) === null || _j === void 0 ? void 0 : _j.replace(new RegExp(`^${key}`, "g"), targetWithForcedPrefix.pathname)) === null || _k === void 0 ? void 0 : _k.replace(/^\/*/, "/")}`);
    const downstreamRequestOptions = {
        hostname: target.hostname,
        path: target.pathname,
        port: target.port,
        protocol: target.protocol,
        rejectUnauthorized: false,
        method: request.method,
        headers: Object.assign(Object.assign({}, request.headers), { host: target.hostname, origin: target.origin }),
        host: target.hostname,
    };
    const downstreamRequest = target.protocol === "https:"
        ? (0, https_1.request)(downstreamRequestOptions)
        : (0, http_1.request)(downstreamRequestOptions);
    downstreamRequest.end();
    downstreamRequest.on("error", error => {
        state.log([
            [
                {
                    text: `${EMOJIS.WEBSOCKET} websocket request has errored ${error.errno
                        ? `(${error.errno})`
                        : ""}`,
                    color: LogLevel.WARNING,
                },
            ],
        ]);
    });
    downstreamRequest.on("upgrade", (response, downstreamSocket) => {
        const upgradeResponse = `HTTP/${response.httpVersion} ${response.statusCode} ${response.statusMessage}\r\n${Object.entries(response.headers)
            .flatMap(([key, value]) => (!Array.isArray(value) ? [value] : value).map(oneValue => [
            key,
            oneValue,
        ]))
            .map(([key, value]) => `${key}: ${value}\r\n`)
            .join("")}\r\n`;
        upstreamSocket.write(upgradeResponse);
        upstreamSocket.allowHalfOpen = true;
        downstreamSocket.allowHalfOpen = true;
        downstreamSocket.on("data", data => upstreamSocket.write(data));
        upstreamSocket.on("data", data => downstreamSocket.write(data));
        downstreamSocket.on("error", error => {
            state.log([
                [
                    {
                        text: `${EMOJIS.WEBSOCKET} downstream socket has errored ${error.errno
                            ? `(${error.errno})`
                            : ""}`,
                        color: LogLevel.WARNING,
                    },
                ],
            ]);
        });
        upstreamSocket.on("error", error => {
            state.log([
                [
                    {
                        text: `${EMOJIS.WEBSOCKET} upstream socket has errored ${error.errno
                            ? `(${error.errno})`
                            : ""}`,
                        color: LogLevel.WARNING,
                    },
                ],
            ]);
        });
    });
    return {};
};
exports.websocketServe = websocketServe;
const serve = function (state, inboundRequest, inboundResponse) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m, _o, _p, _q, _r, _s, _t, _u, _v, _w, _x, _y, _z, _0, _1, _2, _3;
        // phase: mapping
        if (!inboundRequest.headers.host && !inboundRequest.headers[":authority"]) {
            send(400, inboundResponse, Buffer.from(errorPage(new Error(`client must supply a 'host' header`), state.mode, "proxy", new url_1.URL(`http${state.config.ssl ? "s" : ""}://unknowndomain${inboundRequest.url}`))));
            return;
        }
        const { proxyHostname, proxyHostnameAndPort, url, path, key, target: targetFromProxy, } = determineMapping(inboundRequest, state.config);
        const proxyOrigin = `http${state.config.ssl ? "s" : ""}://${proxyHostnameAndPort}`;
        let referrerOrigin = null;
        try {
            referrerOrigin = new url_1.URL(inboundRequest.headers["referer"]).origin;
        }
        catch (e) { }
        const target = targetFromProxy !== null && targetFromProxy !== void 0 ? targetFromProxy : (state.mode === ServerMode.MOCK ? new url_1.URL(proxyOrigin) : null);
        if (!target) {
            send(502, inboundResponse, Buffer.from(errorPage(new Error(`No mapping found in config file ${filename}`), state.mode, "proxy", url)));
            return;
        }
        const protocolSlashes = target.protocol === "data:" ? "" : "//";
        const targetHost = target.host.replace(RegExp(/\/+$/), "");
        const targetPrefix = target.href.substring(`${target.protocol}${protocolSlashes}`.length + target.host.length);
        const fullPath = target.protocol === "file:" || target.protocol === "data:"
            ? targetPrefix
            : `${targetPrefix}${unixNorm(path.replace(RegExp(unixNorm(key)), ""))}`.replace(/^\/*/, target.protocol === "data:" ? "" : "/");
        const targetUrl = new url_1.URL(`${target.protocol}${protocolSlashes}${targetHost}${fullPath}`);
        const targetUsesSpecialProtocol = specialProtocols.some(protocol => target.protocol === protocol);
        const randomId = (0, crypto_1.randomBytes)(20).toString("hex");
        let requestBody = null;
        const bufferedRequestBody = state.config.replaceRequestBodyUrls || !!state.logsListeners.length;
        // sounds ridiculous, but yes, I need to wait until the HTTP/2 stream gets read
        if (state.config.ssl)
            yield new Promise(resolve => setTimeout(resolve, 1));
        const hasImmediateOrDeferredRequestBody = parseInt((_a = inboundRequest.headers["content-length"]) !== null && _a !== void 0 ? _a : "0") > 0;
        const http1WithRequestBody = !!(inboundRequest === null || inboundRequest === void 0 ? void 0 : inboundRequest.readableLength) ||
            hasImmediateOrDeferredRequestBody;
        const http2WithRequestBody = !!(inboundRequest === null || inboundRequest === void 0 ? void 0 : inboundRequest.stream) &&
            hasImmediateOrDeferredRequestBody;
        const serverSentEvents = !!((_c = (_b = inboundRequest.headers) === null || _b === void 0 ? void 0 : _b.accept) === null || _c === void 0 ? void 0 : _c.includes("text/event-stream"));
        const requestBodyExpected = !(((state.config.ssl && http2WithRequestBody === false) ||
            (!state.config.ssl && http1WithRequestBody === false)) &&
            (inboundRequest.headers["content-length"] === "0" ||
                inboundRequest.headers["content-length"] === undefined));
        if (bufferedRequestBody) {
            // this is optional,
            // I don't want to buffer request bodies if
            // none of the options are activated
            const requestBodyReadable = (_d = inboundRequest === null || inboundRequest === void 0 ? void 0 : inboundRequest.stream) !== null && _d !== void 0 ? _d : inboundRequest;
            let requestBodyBuffer = Buffer.from([]);
            yield Promise.race([
                new Promise(resolve => setTimeout(resolve, state.config.connectTimeout)),
                new Promise(resolve => {
                    if (!requestBodyExpected) {
                        resolve(void 0);
                        return;
                    }
                    requestBodyReadable.on("data", chunk => {
                        requestBodyBuffer = Buffer.concat([requestBodyBuffer, chunk]);
                    });
                    requestBodyReadable.on("end", resolve);
                    requestBodyReadable.on("error", resolve);
                }),
            ]);
            if (requestBodyExpected && !requestBodyBuffer.length)
                yield state.log([
                    [
                        {
                            text: `${EMOJIS.ERROR_4} body replacement error ${path.slice(-17)}`,
                            color: LogLevel.WARNING,
                        },
                    ],
                ]);
            requestBody = !state.config.replaceRequestBodyUrls
                ? requestBodyBuffer
                : yield replaceBody(requestBodyBuffer, inboundRequest.headers, {
                    proxyHostnameAndPort,
                    proxyHostname,
                    key,
                    mapping: (_e = state.config.mapping) !== null && _e !== void 0 ? _e : {},
                    port: (_f = state.config.port) !== null && _f !== void 0 ? _f : defaultConfig.port,
                    ssl: !!state.config.ssl,
                    direction: REPLACEMENT_DIRECTION.OUTBOUND,
                });
        }
        const atLeastOneLoggerWantsResponseBody = state.logsListeners.some(listener => listener.wantsResponseMessage);
        const autoRecordModeEnabled = state.mockConfig.autoRecord && state.mode === ServerMode.PROXY;
        const uniqueHash = cleanEntropy(state.config, {
            method: (_g = inboundRequest.method) !== null && _g !== void 0 ? _g : "GET",
            url: (_h = inboundRequest.url) !== null && _h !== void 0 ? _h : "",
            headers: Object.assign({}, ...Object.entries(inboundRequest.headers)
                .filter(([headerName]) => !headerName.startsWith(":"))
                .map(([key, value]) => ({ [key]: value }))),
            body: state.mode === ServerMode.MOCK ||
                atLeastOneLoggerWantsResponseBody ||
                autoRecordModeEnabled
                ? (_j = requestBody === null || requestBody === void 0 ? void 0 : requestBody.toString("base64")) !== null && _j !== void 0 ? _j : ""
                : "",
        });
        if (state.config.logAccessInTerminal &&
            !targetUrl.pathname.startsWith("/:/")) {
            const requestMethodLength = ((_l = (_k = inboundRequest.method) === null || _k === void 0 ? void 0 : _k.length) !== null && _l !== void 0 ? _l : 3) + 2;
            const keyToDisplay = state.config.logAccessInTerminal === "with-mapping" ? key !== null && key !== void 0 ? key : "" : "";
            const keyLength = keyToDisplay.length
                ? Math.min(screenWidth - requestMethodLength - 2, keyToDisplay.length + 2)
                : 0;
            const requestLength = Math.max(2, screenWidth - requestMethodLength - keyLength);
            yield state.log([
                [
                    {
                        color: (_o = {
                            GET: 22,
                            POST: 52,
                            PUT: 94,
                            DELETE: 244,
                            OPTIONS: 19,
                            PATCH: 162,
                            HEAD: 53,
                            TRACE: 6,
                            CONNECT: 2,
                        }[(_m = inboundRequest.method) !== null && _m !== void 0 ? _m : ""]) !== null && _o !== void 0 ? _o : 0,
                        text: ((_p = inboundRequest.method) !== null && _p !== void 0 ? _p : "GET").toString(),
                        length: requestMethodLength - 2,
                    },
                    {
                        color: 32,
                        text: keyToDisplay.substring(0, keyLength - 2),
                        length: keyLength - 2,
                    },
                    {
                        color: 8,
                        text: targetUrl.pathname
                            .toString()
                            .padStart(requestLength)
                            .substring(0, requestLength),
                        length: requestLength,
                    },
                ],
            ]);
        }
        const shouldMock = state.mode === ServerMode.MOCK && !targetUsesSpecialProtocol;
        const foundMock = !shouldMock
            ? null
            : (_q = state.mockConfig.mocks.get(uniqueHash)) !== null && _q !== void 0 ? _q : (_r = Array.from(state.mockConfig.mocks.entries())
                .filter(([hash]) => {
                var _a;
                const requestObject = JSON.parse(Buffer.from(uniqueHash, "base64").toString("ascii"));
                const mockRequestObject = JSON.parse(Buffer.from(hash, "base64").toString("ascii"));
                return (mockRequestObject.method === requestObject.method &&
                    mockRequestObject.url === requestObject.url &&
                    (!mockRequestObject.body ||
                        mockRequestObject.body === requestObject.body) &&
                    Object.entries((_a = mockRequestObject.headers) !== null && _a !== void 0 ? _a : {}).every(([name, value]) => {
                        var _a, _b, _c, _d;
                        return !value ||
                            ((_c = (_b = (_a = state.config) === null || _a === void 0 ? void 0 : _a.unwantedHeaderNamesInMocks) === null || _b === void 0 ? void 0 : _b.includes) === null || _c === void 0 ? void 0 : _c.call(_b, name)) ||
                            ((_d = requestObject.headers) === null || _d === void 0 ? void 0 : _d[name]) === value;
                    }));
            })
                .sort(([hash1], [hash2]) => {
                var _a, _b;
                const match2RequestObject = JSON.parse(Buffer.from(hash2, "base64").toString("ascii"));
                const match1RequestObject = JSON.parse(Buffer.from(hash1, "base64").toString("ascii"));
                const match2HasBody = match2RequestObject.body ? 1 : 0;
                const match1HasBody = match1RequestObject.body ? 1 : 0;
                return (Object.keys((_a = match2RequestObject.headers) !== null && _a !== void 0 ? _a : {}).length +
                    match2HasBody -
                    Object.keys((_b = match1RequestObject.headers) !== null && _b !== void 0 ? _b : {}).length -
                    match1HasBody);
            })[0]) === null || _r === void 0 ? void 0 : _r[1];
        if (shouldMock && !foundMock && state.mockConfig.strict) {
            send(502, inboundResponse, Buffer.from(errorPage(new Error(`No corresponding mock found in the server. 
          Try switching back to the proxy mode`), state.mode, "mock", url)));
            return;
        }
        // phase: connection
        let error = null;
        const startTime = instantTime();
        const outboundHeaders = Object.assign(Object.assign({}, [...Object.entries(inboundRequest.headers)]
            // host, connection and keep-alive are forbidden in http/2
            .filter(([key]) => !disallowedHttp2HeaderNames.includes(key.toLowerCase()))
            .reduce((acc, [key, value]) => {
            acc[key] =
                (acc[key] || "") +
                    (!Array.isArray(value) ? [value] : value)
                        .map(oneValue => oneValue === null || oneValue === void 0 ? void 0 : oneValue.replace(url.hostname, targetHost))
                        .join(", ");
            return acc;
        }, {})), { origin: target.href, referer: targetUrl.toString(), "content-length": (_t = (_s = requestBody === null || requestBody === void 0 ? void 0 : requestBody.length) !== null && _s !== void 0 ? _s : inboundRequest.headers["content-length"]) !== null && _t !== void 0 ? _t : 0, ":authority": targetHost, ":method": inboundRequest.method, ":path": fullPath, ":scheme": target.protocol.replace(":", "") });
        const outboundRequest = shouldMock && foundMock
            ? mockRequest({ response: foundMock })
            : targetUsesSpecialProtocol
                ? specialPageMapping[target.protocol.replace(/:$/, "")](state, inboundRequest, {
                    target: targetUrl,
                    url,
                    proxyHostnameAndPort,
                    proxyHostname,
                    key,
                    requestBody,
                    proxyOrigin,
                })
                : yield http2Page(state, {
                    target: targetUrl,
                    url,
                })
                    .then(session => {
                    if (session)
                        return session;
                    return http1Page(target, url, targetUrl, fullPath, inboundRequest, outboundHeaders, requestBody, bufferedRequestBody, state.mode);
                })
                    .catch(e => {
                    error = e;
                    return null;
                });
        const protocol = ((_v = (_u = outboundRequest === null || outboundRequest === void 0 ? void 0 : outboundRequest.alpnProtocol) === null || _u === void 0 ? void 0 : _u.startsWith) === null || _v === void 0 ? void 0 : _v.call(_u, "h2"))
            ? "HTTP/2"
            : (_w = outboundRequest === null || outboundRequest === void 0 ? void 0 : outboundRequest.alpnProtocol) !== null && _w !== void 0 ? _w : "HTTP1.1";
        state.notifyLogsListeners({
            level: "info",
            protocol,
            method: inboundRequest.method,
            upstreamPath: path,
            downstreamPath: targetUrl.href,
            randomId,
            uniqueHash,
        });
        if (!(error instanceof Buffer))
            error = null;
        const outboundExchange = !error &&
            (outboundRequest === null || outboundRequest === void 0 ? void 0 : outboundRequest.request(outboundHeaders, {
                endStream: state.config.ssl
                    ? !(http2WithRequestBody !== null && http2WithRequestBody !== void 0 ? http2WithRequestBody : true)
                    : !http1WithRequestBody,
            }));
        typeof outboundExchange === "object" &&
            ((_x = outboundExchange === null || outboundExchange === void 0 ? void 0 : outboundExchange.on) === null || _x === void 0 ? void 0 : _x.call(outboundExchange, "error", (thrown) => {
                const httpVersionSupported = thrown.errno === -505;
                error = Buffer.from(errorPage(thrown, state.mode, "stream" +
                    (httpVersionSupported
                        ? " (error -505 usually means that the downstream service " +
                            "does not support this http version)"
                        : ""), url, targetUrl));
            }));
        // intriguingly, error is reset to "false" at this point, even if it was null
        if (error) {
            send(502, inboundResponse, error);
            return;
        }
        else
            error = null;
        // phase : request body
        if (http2WithRequestBody && outboundExchange && !bufferedRequestBody) {
            inboundRequest.stream.on("data", chunk => {
                outboundExchange.write(chunk);
            });
            inboundRequest.stream.on("end", () => outboundExchange.end());
        }
        else if (http1WithRequestBody && outboundExchange && !bufferedRequestBody) {
            inboundRequest.on("data", chunk => {
                outboundExchange.write(chunk);
            });
            inboundRequest.on("end", () => outboundExchange.end());
        }
        else if (outboundExchange &&
            bufferedRequestBody &&
            requestBodyExpected &&
            !outboundExchange.writableEnded) {
            outboundExchange.write(requestBody);
            outboundExchange.end();
        }
        // phase : response headers
        const { outboundResponseHeaders } = yield new Promise(resolve => {
            var _a, _b;
            return (_b = (_a = outboundExchange === null || outboundExchange === void 0 ? void 0 : outboundExchange.on) === null || _a === void 0 ? void 0 : _a.call(outboundExchange, "response", headers => {
                resolve({
                    outboundResponseHeaders: headers,
                });
            })) !== null && _b !== void 0 ? _b : resolve({ outboundResponseHeaders: {} });
        });
        let redirectUrl = null;
        try {
            if (outboundResponseHeaders["location"])
                redirectUrl = new url_1.URL(outboundResponseHeaders["location"].startsWith("/")
                    ? `${target.origin}${outboundResponseHeaders["location"].replace(/^\/+/, `/`)}`
                    : outboundResponseHeaders["location"]
                        .replace(/^file:\/+/, "file:///")
                        .replace(/^(http)(s?):\/+/, "$1$2://"));
        }
        catch (e) {
            yield state.log([
                [
                    {
                        text: `${EMOJIS.ERROR_4} location replacement error ${((_y = outboundResponseHeaders["location"]) !== null && _y !== void 0 ? _y : "").slice(-13)}`,
                        color: LogLevel.WARNING,
                    },
                ],
            ]);
        }
        const replacedRedirectUrl = !state.config.replaceResponseBodyUrls || !redirectUrl
            ? redirectUrl
            : new url_1.URL(replaceTextUsingMapping(redirectUrl.href, {
                direction: REPLACEMENT_DIRECTION.INBOUND,
                proxyHostnameAndPort,
                ssl: !!state.config.ssl,
                mapping: (_z = state.config.mapping) !== null && _z !== void 0 ? _z : {},
            }).replace(new RegExp(`^(${specialProtocols.join("|")})\/+`), ""));
        const translatedReplacedRedirectUrl = !redirectUrl
            ? redirectUrl
            : (replacedRedirectUrl === null || replacedRedirectUrl === void 0 ? void 0 : replacedRedirectUrl.origin) !== redirectUrl.origin ||
                state.config.dontTranslateLocationHeader
                ? replacedRedirectUrl
                : `${url.origin}${replacedRedirectUrl.href.substring(replacedRedirectUrl.origin.length)}`;
        // phase : response body
        const payload = error !== null && error !== void 0 ? error : (yield new Promise(resolve => {
            var _a, _b;
            let partialBody = Buffer.alloc(0);
            if (!outboundExchange) {
                resolve(partialBody);
                return;
            }
            (_a = outboundExchange === null || outboundExchange === void 0 ? void 0 : outboundExchange.on) === null || _a === void 0 ? void 0 : _a.call(outboundExchange, "data", (chunk) => {
                partialBody = Buffer.concat([
                    partialBody,
                    typeof chunk === "string"
                        ? Buffer.from(chunk)
                        : chunk,
                ]);
                if (serverSentEvents)
                    resolve(partialBody);
            });
            (_b = outboundExchange === null || outboundExchange === void 0 ? void 0 : outboundExchange.on) === null || _b === void 0 ? void 0 : _b.call(outboundExchange, "end", () => {
                resolve(partialBody);
            });
        }).then((payloadBuffer) => {
            var _a, _b;
            if (!state.config.replaceResponseBodyUrls)
                return payloadBuffer;
            if (!payloadBuffer.length)
                return payloadBuffer;
            if (specialProtocols.some(protocol => target.protocol === protocol))
                return payloadBuffer;
            return replaceBody(payloadBuffer, outboundResponseHeaders, {
                proxyHostnameAndPort,
                proxyHostname,
                key,
                direction: REPLACEMENT_DIRECTION.INBOUND,
                mapping: (_a = state.config.mapping) !== null && _a !== void 0 ? _a : {},
                port: (_b = state.config.port) !== null && _b !== void 0 ? _b : defaultConfig.port,
                ssl: !!state.config.ssl,
            }).catch((e) => {
                send(502, inboundResponse, Buffer.from(errorPage(e, state.mode, "stream", url, targetUrl)));
                return Buffer.from("");
            });
        }));
        // phase : inbound response
        const responseHeaders = Object.assign(Object.assign({}, Object.entries(Object.assign(Object.assign(Object.assign(Object.assign({}, outboundResponseHeaders), (state.config.replaceResponseBodyUrls && !serverSentEvents
            ? { ["content-length"]: `${payload.byteLength}` }
            : {})), (state.config.disableWebSecurity
            ? {
                ["content-security-policy"]: "report only",
                ["access-control-allow-headers"]: "*",
                ["access-control-allow-method"]: "*",
                ["access-control-allow-origin"]: referrerOrigin !== null && referrerOrigin !== void 0 ? referrerOrigin : "*",
                ["access-control-allow-credentials"]: "true",
                ["x-frame-options"]: "SAMEORIGIN",
            }
            : {})), (serverSentEvents
            ? {
                ["cache-control"]: "no-cache",
                ["x-accel-buffering"]: "no",
            }
            : {})))
            .filter(([h]) => !h.startsWith(":") &&
            !disallowedHttp2HeaderNames.includes(h.toLowerCase()))
            .reduce((acc, [key, value]) => {
            const allSubdomains = targetHost
                .split("")
                .map((_, i) => targetHost.substring(i).startsWith(".") &&
                targetHost.substring(i))
                .filter(subdomain => subdomain);
            const transformedValue = [targetHost].concat(allSubdomains).reduce((acc1, subDomain) => (!Array.isArray(acc1) ? [acc1] : acc1).map(oneElement => {
                return typeof oneElement === "string"
                    ? oneElement.replace(`Domain=${subDomain}`, `Domain=${url.hostname}`)
                    : oneElement;
            }), value);
            acc[key] = (acc[key] || []).concat(transformedValue);
            return acc;
        }, {})), (translatedReplacedRedirectUrl
            ? { location: [translatedReplacedRedirectUrl] }
            : {}));
        try {
            Object.entries(responseHeaders).forEach(([headerName, headerValue]) => headerValue &&
                inboundResponse.setHeader(headerName, headerValue));
        }
        catch (e) {
            // ERR_HTTP2_HEADERS_SENT
        }
        const statusCode = (_0 = outboundResponseHeaders[":status"]) !== null && _0 !== void 0 ? _0 : 200;
        try {
            if (state.config.ssl) {
                inboundResponse.writeHead(statusCode, responseHeaders);
            }
            else {
                inboundResponse.writeHead(statusCode, protocol === "HTTP/2"
                    ? ""
                    : (_2 = (_1 = outboundResponseHeaders[":statusmessage"]) === null || _1 === void 0 ? void 0 : _1.toString()) !== null && _2 !== void 0 ? _2 : "", responseHeaders);
            }
        }
        catch (e) { }
        if (serverSentEvents) {
            inboundResponse.on("close", () => {
                var _a;
                try {
                    (_a = outboundExchange === null || outboundExchange === void 0 ? void 0 : outboundExchange.close) === null || _a === void 0 ? void 0 : _a.call(outboundExchange);
                }
                catch (e) { }
            });
            inboundResponse.write(payload);
            (_3 = outboundExchange === null || outboundExchange === void 0 ? void 0 : outboundExchange.on) === null || _3 === void 0 ? void 0 : _3.call(outboundExchange, "data", (chunk) => inboundResponse.write(chunk));
        }
        else if (payload)
            inboundResponse.end(payload);
        else
            inboundResponse.end();
        const endTime = instantTime();
        const response = atLeastOneLoggerWantsResponseBody || autoRecordModeEnabled
            ? Buffer.from(JSON.stringify({
                body: payload === null || payload === void 0 ? void 0 : payload.toString("base64"),
                headers: outboundResponseHeaders,
                status: statusCode,
            })).toString("base64")
            : "";
        state.notifyLogsListeners({
            randomId,
            statusCode,
            protocol,
            duration: Math.floor(Number(endTime - startTime) / 1000000),
            uniqueHash,
            response,
        });
        // not using quick status if logAccessInTerminal is enabled
        if (autoRecordModeEnabled &&
            !state.config.logAccessInTerminal &&
            !targetUsesSpecialProtocol) {
            state.mockConfig.mocks.set(uniqueHash, response);
            process_1.stdout.moveCursor(0, -1, () => process_1.stdout.clearLine(-1, state.quickStatus));
        }
    });
};
exports.serve = serve;
const errorListener = (state, err) => {
    if (err.code === "EACCES")
        setTimeout(() => state.log([
            [
                {
                    text: `${EMOJIS.NO} permission denied for this port`,
                    color: LogLevel.ERROR,
                },
            ],
        ]), 10);
    if (err.code === "EADDRINUSE")
        setTimeout(() => state.log([
            [
                {
                    text: `${EMOJIS.ERROR_6} port is already used. NOT started`,
                    color: LogLevel.ERROR,
                },
            ],
        ]), 10);
};
exports.errorListener = errorListener;
const start = (config) => update({ config: Object.assign(Object.assign({}, defaultConfig), config) }, { server: null });
exports.start = start;
const update = (currentState, newState) => __awaiter(void 0, void 0, void 0, function* () {
    var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m, _o, _p, _q, _r, _s, _t, _u, _v, _w, _x, _y, _z, _0, _1;
    if (Object.keys(newState !== null && newState !== void 0 ? newState : {}).length === 0 && currentState.server)
        return newState;
    if (newState === null || newState === void 0 ? void 0 : newState.pendingConfigSave) {
        (0, fs_1.writeFile)(filename, JSON.stringify(newState.pendingConfigSave, null, 2), fileWriteErr => {
            var _a, _b;
            if (fileWriteErr)
                (_a = currentState.log) === null || _a === void 0 ? void 0 : _a.call(currentState, [
                    [
                        {
                            text: `${EMOJIS.ERROR_4} config file NOT saved`,
                            color: LogLevel.ERROR,
                        },
                    ],
                ]);
            else
                (_b = currentState.log) === null || _b === void 0 ? void 0 : _b.call(currentState, [
                    [
                        {
                            text: `${EMOJIS.COLORED} config file saved... will reload`,
                            color: LogLevel.INFO,
                        },
                    ],
                ]);
        });
        return currentState;
    }
    if ((newState === null || newState === void 0 ? void 0 : newState.configListeners) === null) {
        yield Promise.all(((_a = currentState.configListeners) !== null && _a !== void 0 ? _a : []).map(listener => new Promise(resolve => listener.stream.end(resolve))));
    }
    if ((newState === null || newState === void 0 ? void 0 : newState.logsListeners) === null) {
        yield Promise.all(((_b = currentState.configListeners) !== null && _b !== void 0 ? _b : []).map(listener => new Promise(resolve => listener.stream.end(resolve))));
    }
    if ((newState === null || newState === void 0 ? void 0 : newState.server) === null && currentState.server) {
        const stopped = yield Promise.race([
            new Promise(resolve => { var _a; return (_a = currentState.server) === null || _a === void 0 ? void 0 : _a.close(resolve); }).then(() => true),
            new Promise(resolve => setTimeout(resolve, 5000)).then(() => false),
        ]);
        if (!stopped) {
            yield ((_c = currentState.log) === null || _c === void 0 ? void 0 : _c.call(currentState, [
                [
                    {
                        text: `${EMOJIS.RESTART} error during restart (websockets ?)`,
                        color: LogLevel.WARNING,
                    },
                ],
            ]));
        }
    }
    ((_d = currentState.configListeners) !== null && _d !== void 0 ? _d : [])
        .concat((_e = currentState.logsListeners) !== null && _e !== void 0 ? _e : [])
        .filter(l => l.stream.errored || l.stream.closed)
        .forEach(l => l.stream.destroy());
    const config = (_f = newState === null || newState === void 0 ? void 0 : newState.config) !== null && _f !== void 0 ? _f : currentState.config;
    const mode = (_h = (_g = newState === null || newState === void 0 ? void 0 : newState.mode) !== null && _g !== void 0 ? _g : currentState.mode) !== null && _h !== void 0 ? _h : ServerMode.PROXY;
    const autoRecord = (_m = (_k = (_j = newState === null || newState === void 0 ? void 0 : newState.mockConfig) === null || _j === void 0 ? void 0 : _j.autoRecord) !== null && _k !== void 0 ? _k : (_l = currentState.mockConfig) === null || _l === void 0 ? void 0 : _l.autoRecord) !== null && _m !== void 0 ? _m : false;
    const strict = (_r = (_p = (_o = newState === null || newState === void 0 ? void 0 : newState.mockConfig) === null || _o === void 0 ? void 0 : _o.strict) !== null && _p !== void 0 ? _p : (_q = currentState.mockConfig) === null || _q === void 0 ? void 0 : _q.strict) !== null && _r !== void 0 ? _r : false;
    const mocks = (_v = (_t = (_s = newState === null || newState === void 0 ? void 0 : newState.mockConfig) === null || _s === void 0 ? void 0 : _s.mocks) !== null && _t !== void 0 ? _t : (_u = currentState.mockConfig) === null || _u === void 0 ? void 0 : _u.mocks) !== null && _v !== void 0 ? _v : new Map();
    const configListeners = ((newState === null || newState === void 0 ? void 0 : newState.configListeners) === null
        ? []
        : (_x = (_w = newState === null || newState === void 0 ? void 0 : newState.configListeners) !== null && _w !== void 0 ? _w : currentState.configListeners) !== null && _x !== void 0 ? _x : []).filter(l => !l.stream.errored && !l.stream.closed);
    const logsListeners = ((newState === null || newState === void 0 ? void 0 : newState.logsListeners) === null
        ? []
        : (_z = (_y = newState === null || newState === void 0 ? void 0 : newState.logsListeners) !== null && _y !== void 0 ? _y : currentState.logsListeners) !== null && _z !== void 0 ? _z : []).filter(l => !l.stream.errored && !l.stream.closed);
    const state = currentState;
    Object.assign(state, {
        config,
        logsListeners,
        configListeners,
        mode,
        mockConfig: {
            mocks,
            strict,
            autoRecord,
        },
        configFileWatcher: state.configFileWatcher === undefined
            ? (0, fs_1.watchFile)(filename, (stats) => __awaiter(void 0, void 0, void 0, function* () {
                update(state, stats.isFile() ? yield onWatch(state) : { server: null });
            }))
            : state.configFileWatcher,
        log: log.bind(state, state),
        notifyConfigListeners: notifyConfigListeners.bind(state),
        notifyLogsListeners: notifyLogsListeners.bind(state),
        buildQuickStatus: buildQuickStatus.bind(state),
        quickStatus: quickStatus.bind(state),
        server: (newState === null || newState === void 0 ? void 0 : newState.server) === null && !(((_1 = (_0 = newState === null || newState === void 0 ? void 0 : newState.config) === null || _0 === void 0 ? void 0 : _0.port) !== null && _1 !== void 0 ? _1 : 0) < 0)
            ? ((config === null || config === void 0 ? void 0 : config.ssl)
                ? http2_1.createSecureServer.bind(null, Object.assign(Object.assign({}, config.ssl), { allowHTTP1: true }))
                : http_1.createServer)((request, response) => serve(state, request, response))
                .addListener("error", (error) => errorListener(state, error))
                .on("upgrade", (request, socket) => update(state, websocketServe(state, request, socket)))
                .listen(config === null || config === void 0 ? void 0 : config.port)
            : !(newState === null || newState === void 0 ? void 0 : newState.server)
                ? null
                : state.server,
    });
    return state;
});
exports.update = update;
if (crashTest) {
    const port = Math.floor(40151 + Math.random() * 9000);
    const makeRequest = (state, resolve) => (0, http_1.request)({
        hostname: "localhost",
        port,
        path: "/config/",
        method: "GET",
        headers: {
            Accept: "text/html",
        },
        timeout: 500,
    }, response => resolve({ response, state }))
        .on("error", error => resolve({ error, state }))
        .end();
    update({ config: Object.assign(Object.assign({}, defaultConfig), { port }), configFileWatcher: null }, { server: null })
        .then(state => new Promise(resolve => setTimeout(makeRequest.bind(null, state, resolve), 1000)))
        .then(({ state, response }) => response.statusCode !== 200
        ? Promise.reject("Crash test has failed")
        : update(state, { config: { port: -1 }, server: null }))
        .then(state => new Promise(resolve => setTimeout(makeRequest.bind(null, state, resolve), 1000)))
        .then(({ error }) => (error === null || error === void 0 ? void 0 : error.code) !== "ECONNREFUSED"
        ? Promise.reject("Server should have stopped")
        : log({ config: { simpleLogs: true } }, [
            [
                {
                    text: `${EMOJIS.COLORED} Crash test successful`,
                    color: LogLevel.INFO,
                },
            ],
        ]))
        .then(() => (0, process_1.exit)(0))
        .catch(() => (0, process_1.exit)(1));
}
if (!crashTest && runAsMainProgram) {
    load()
        .then(start)
        .then(state => state.quickStatus());
}
