import { DurableObject } from "cloudflare:workers";

const freshState = () => ({
  phase: "build",
  players: { A: null, B: null },
  log: [],
  activeAction: null,
  processedActions: []
});

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname !== "/ws") {
      return env.ASSETS.fetch(request);
    }
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("WebSocket endpoint. Open the duel page first, then connect from the page.", { status: 426 });
    }
    const room = (url.searchParams.get("room") || "").toUpperCase().replace(/[^A-Z0-9_-]/g, "").slice(0, 20);
    if (!room) return new Response("room required", { status: 400 });
    const id = env.DUEL_ROOMS.idFromName(room);
    return env.DUEL_ROOMS.get(id).fetch(request);
  }
};

export class DuelRoom extends DurableObject {
  constructor(ctx, env) { super(ctx, env); this.ctx = ctx; this.env = env; }
  async load() { return (await this.ctx.storage.get("state")) || freshState(); }
  async save(s) { await this.ctx.storage.put("state", s); }
  send(ws, obj) { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }
  broadcast(obj) { for (const s of this.ctx.getWebSockets()) this.send(s, obj); }
  socketForRole(role) {
    return this.ctx.getWebSockets().find(s => (s.deserializeAttachment() || {}).role === role);
  }
  publicPlayers(state) {
    const out = {};
    for (const k of ["A", "B"]) {
      const p = state.players[k];
      out[k] = p ? {
        name: p.name,
        team: p.team || [],
        eliminated: p.eliminated || [],
        confirmed: !!p.confirmed
      } : null;
    }
    return out;
  }
  actionIsProcessed(state, id) { return !!id && (state.processedActions || []).includes(id); }
  markProcessed(state, id) {
    if (!id) return;
    state.processedActions = state.processedActions || [];
    if (!state.processedActions.includes(id)) state.processedActions.push(id);
  }
  async emitBattle(state, message) {
    if (message) state.log.push(message);
    await this.save(state);
    this.broadcast({
      type: "battle_state",
      players: this.publicPlayers(state),
      log: state.log,
      processedActions: state.processedActions || [],
      phase: state.phase,
      activeAction: state.activeAction
    });
  }
  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") return new Response("Expected WebSocket", { status: 400 });
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ session: crypto.randomUUID(), role: null });
    return new Response(null, { status: 101, webSocket: client });
  }
  async webSocketMessage(ws, message) {
    let m; try { m = JSON.parse(message); } catch { return; }
    const state = await this.load();
    state.processedActions = state.processedActions || [];
    const att = ws.deserializeAttachment() || {};
    const sockets = this.ctx.getWebSockets();

    if (m.type === "join") {
      let role = att.role || null;
      if (!role) {
        if (!state.players.A) role = "A";
        else if (!state.players.B) role = "B";
        else return this.send(ws, { type: "error", message: "房间已满。" });
      }
      ws.serializeAttachment({ session: att.session, role });
      if (!state.players[role]) state.players[role] = { session: att.session, name: m.name || "玩家", team: [], eliminated: [], confirmed: false };
      else {
        state.players[role].session = att.session;
        state.players[role].name = m.name || state.players[role].name;
        state.players[role].eliminated = state.players[role].eliminated || [];
      }
      await this.save(state);
      this.send(ws, { type: "joined", room: m.room || "", role, phase: state.phase, team: state.players[role].team || [], confirmed: !!state.players[role].confirmed, eliminated: state.players[role].eliminated || [], message: "已进入房间。" });
      for (const s of sockets) if (s !== ws) this.send(s, { type: "opponent_status", confirmed: !!state.players[role].confirmed });
      return;
    }

    const role = att.role;
    if (!role || !state.players[role]) return this.send(ws, { type: "error", message: "尚未加入房间。" });

    if (m.type === "team_update") {
      if (state.phase !== "build" || state.players[role].confirmed) return;
      const team = Array.isArray(m.team) ? m.team.map(String).filter((v, i, a) => a.indexOf(v) === i).slice(0, 5) : [];
      const other = role === "A" ? "B" : "A";
      const otherTeam = state.players[other]?.team || [];
      if (team.some(id => otherTeam.includes(id))) return this.send(ws, { type: "error", message: "同一宝可梦不能同时被双方选入阵容。" });
      state.players[role].team = team;
      await this.save(state);
      this.send(ws, { type: "state", team, confirmed: false, phase: "build", eliminated: state.players[role].eliminated || [], message: "阵容已保存，对方看不到你的成员。" });
      return;
    }

    if (m.type === "confirm") {
      if (state.phase !== "build" || state.players[role].team.length !== 5) return this.send(ws, { type: "error", message: "必须先组成完整 5 人阵容。" });
      state.players[role].confirmed = true;
      const both = state.players.A && state.players.B && state.players.A.confirmed && state.players.B.confirmed;
      if (both) {
        state.phase = "reveal";
        await this.save(state);
        this.broadcast({ type: "reveal", players: this.publicPlayers(state), phase: state.phase });
      } else {
        await this.save(state);
        this.send(ws, { type: "state", team: state.players[role].team, confirmed: true, phase: "build", eliminated: state.players[role].eliminated || [], message: "已确认，等待对方确认。" });
        for (const s of sockets) if (s !== ws) this.send(s, { type: "opponent_status", confirmed: true });
      }
      return;
    }

    if (m.type === "start_battle") {
      if (state.phase !== "reveal") return;
      state.phase = "battle";
      state.activeAction = null;
      await this.emitBattle(state, "双方已明牌。基础羁绊与可确定的“视为其他羁绊”效果进入自动结算。需要玩家决定的效果由裁判 UI 逐项处理。", false);
      return;
    }

    if (m.type === "roll") {
      if (state.phase !== "battle") return this.send(ws, { type: "error", message: "当前不在主动效果结算阶段。" });
      if (state.activeAction) return this.send(ws, { type: "error", message: "已有主动效果正在处理。" });
      const a = m.actionId || `${role}|${m.poke || ""}|${m.bond || ""}`;
      if (this.actionIsProcessed(state, a)) return this.send(ws, { type: "error", message: "该主动效果本轮已经处理过。" });
      const dice = 1 + Math.floor(Math.random() * 6);
      state.activeAction = { type: "roll", owner: role, bond: m.bond || "", poke: m.poke || "", actionId: a, dice };
      await this.save(state);
      this.send(ws, { type: "dice_result", dice, role, bond: m.bond, poke: m.poke, actionId: a });
      for (const s of sockets) if (s !== ws) this.send(s, { type: "action_request", role, kind: "remote_effect", message: "对方正在处理主动效果，请等待结果。" });
      return;
    }

    if (m.type === "resolve_action") {
      const a = m.action || {};
      if (a.kind === "finish") {
        if (state.phase !== "battle") return;
        if (state.activeAction) return this.send(ws, { type: "error", message: "仍有主动效果正在处理。" });
        state.phase = "final";
        await this.emitBattle(state, "所有当前需要玩家处理的主动效果均已完成，裁判进入最终阵容。", false);
        return;
      }
      if (state.phase !== "battle") return this.send(ws, { type: "error", message: "当前没有可处理的主动效果。" });

      if (a.kind === "skip" || a.kind === "trigger") {
        if (state.activeAction) return this.send(ws, { type: "error", message: "请先完成当前主动效果。" });
        const id = a.actionId || `${role}|${a.poke || ""}|${a.bond || ""}`;
        if (this.actionIsProcessed(state, id)) return this.send(ws, { type: "error", message: "该效果已经处理过。" });
        this.markProcessed(state, id);
        const text = `${state.players[role].name || "玩家"} ${a.bond || "效果"}${a.kind === "skip" ? "：选择不触发" : "：选择触发，进入后续结算"}`;
        await this.emitBattle(state, text, false);
        return;
      }

      if (a.kind === "kill") {
        const active = state.activeAction;
        if (!active || active.owner !== role || active.bond !== "激流送葬") return this.send(ws, { type: "error", message: "当前没有属于你的激流送葬斩杀操作。" });
        if (active.dice < 4) return this.send(ws, { type: "error", message: "本次点数不足 4，不能斩杀。" });
        const targetSide = role === "A" ? "B" : "A";
        if (a.targetSide !== targetSide) return this.send(ws, { type: "error", message: "只能选择对方目标。" });
        const target = String(a.targetId || "");
        const tp = state.players[targetSide];
        const idx = tp.team.indexOf(target);
        if (idx < 0) return this.send(ws, { type: "error", message: "目标已经不存在。" });
        tp.team.splice(idx, 1);
        tp.eliminated = tp.eliminated || [];
        if (!tp.eliminated.includes(target)) tp.eliminated.push(target);
        tp.confirmed = false;
        state.activeAction = null;
        state.phase = "replacement";
        state.log.push(`${state.players[role].name || "玩家"} 使用激流送葬斩杀了 ${target}。该宝可梦本局永久淘汰。`);
        await this.save(state);
        this.broadcast({ type: "battle_state", players: this.publicPlayers(state), log: state.log, processedActions: state.processedActions, phase: state.phase, activeAction: null });
        const targetSocket = this.socketForRole(targetSide);
        this.send(targetSocket, { type: "action_request", role: targetSide, kind: "replacement", side: targetSide, message: "你的宝可梦被斩杀，请选择一名新的宝可梦加入阵容。" });
        return;
      }

      if (a.kind === "replace") {
        if (state.phase !== "replacement") return this.send(ws, { type: "error", message: "当前没有替补操作。" });
        if (role !== a.side) return this.send(ws, { type: "error", message: "只有被斩杀的一方可以选择替补。" });
        const id = String(a.newId || "");
        if (!id) return;
        const p = state.players[role], other = role === "A" ? "B" : "A";
        const all = [...(p.team || []), ...(state.players[other].team || []), ...(p.eliminated || []), ...(state.players[other].eliminated || [])];
        if (all.includes(id)) return this.send(ws, { type: "error", message: "该宝可梦已经在阵容中或本局已经被淘汰。" });
        if (p.team.length >= 5) return this.send(ws, { type: "error", message: "当前阵容已经满员。" });
        p.team.push(id);
        state.phase = "battle";
        state.log.push(`${p.name || "玩家"} 选择 ${id} 作为替补，重新计算羁绊。`);
        await this.save(state);
        this.broadcast({ type: "battle_state", players: this.publicPlayers(state), log: state.log, processedActions: state.processedActions, phase: state.phase, activeAction: null });
        return;
      }
    }
  }
  async webSocketClose() {}
  async webSocketError() {}
}
