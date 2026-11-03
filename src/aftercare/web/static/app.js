import { createRefreshQueue } from '/static/refresh_queue.js';
const $=s=>document.querySelector(s);
let developerMode=false;
let csrf='',conversation='',source=null,events=new Map(),lastEvent=0,lastSnapshot=null;
const labels={IDLE:'待开始',RUNNING:'处理中',WAITING_USER:'等待补充',PAUSED_BY_USER:'已按要求暂不提交',WAITING_CONFIRMATION:'等待确认',WAITING_SUBMISSION:'等待提交',SUBMITTED:'已提交 · 告知待完成',EXECUTION_UNKNOWN:'执行结果待核实',REJECTED:'方案已拒绝',TURN_ENDED:'本轮已结束',COMPLETED:'任务已完成',CANCELLED:'已停止',FAILED:'处理失败',HANDOFF_PENDING:'待人工接管',PAID_UNSHIPPED:'已支付 · 未发货',DELIVERED:'已送达',SHIPPED:'运输中',RETURN_REQUESTED:'退货申请已提交',EXCHANGE_REQUESTED:'换货申请已提交',CANCELLED:'已取消'};
const tools={list_orders:'查询订单列表',get_order:'读取订单',get_inventory:'查询实时库存',search_policies:'检索适用政策',update_case:'更新当前诉求',preview_action:'生成操作预览',commit_action:'提交已确认操作',get_operation:'查询执行回执',request_handoff:'创建人工工单'};
const refresher=createRefreshQueue({read:(key,signal)=>api(`/api/conversations/${key}`,{signal}),apply:(snap,key)=>{if(key===conversation)renderSnapshot(snap)}});
let timelineFrame=0;
const kinds={user_message:'收到用户消息',conversation_created:'会话已建立',case_updated:'有效诉求已更新',preview_created:'生成待确认方案',preview_confirmed:'收到明确确认',preview_rejected:'方案已拒绝',operation_committed:'业务事务已提交',handoff_created:'已转人工',job_queued:'任务已入队',job_finished:'处理片段结束',cancel_requested:'已请求停止',demo_fault_injected:'演示故障已设置',invalid_tool_attempt:'参数校验已拦截',forbidden_tool_attempt:'越界工具已拦截'};
function error(e){$('#error').hidden=false;$('#error').textContent=e.message||String(e)}
async function api(path,options={}){const r=await fetch(path,{...options,headers:{'Content-Type':'application/json','X-CSRF-Token':csrf,...options.headers}});let data=await r.json();if(!r.ok)throw Error(data.user_message||data.detail||data.error_code||r.statusText);return data}
function node(tag,text,cls){const el=document.createElement(tag);if(text!==undefined)el.textContent=text;if(cls)el.className=cls;return el}
async function login(){try{let me=await api('/api/demo-sessions',{method:'POST',body:JSON.stringify({customer_id:$('#customer').value})});csrf=me.csrf_token;localStorage.removeItem('aftercare-conversation');await loadOrders();await newConversation()}catch(e){error(e)}}
async function loadOrders(){const data=await api('/api/orders');$('#order-count').textContent=data.length;$('#orders').replaceChildren(...data.map(o=>{const card=node('div',undefined,'order');card.append(node('strong',o.id),node('span',labels[o.status]||o.status),node('p',`¥${(o.paid_fen/100).toFixed(2)} · 点击选择订单`));card.onclick=()=>{$('#message').value=o.id+' ';$('#message').focus()};return card}));}
async function loadConversations(){const items=await api('/api/conversations');$('#conversation').replaceChildren(...items.map(c=>{const option=node('option',c.id.slice(0,16)+' · '+(labels[c.status]||c.status));option.value=c.id;return option}));$('#conversation').value=conversation;}
async function newConversation(){try{conversation=(await api('/api/conversations',{method:'POST'})).id;localStorage.setItem('aftercare-conversation',conversation);await loadConversations();connect();await refresh()}catch(e){error(e)}}
async function selectConversation(id){conversation=id;localStorage.setItem('aftercare-conversation',id);connect();await refresh()}
function connect(){if(source)source.close();refresher.reset(conversation);events=new Map();lastEvent=0;$('#timeline').replaceChildren();openEvents()}
function scheduleTimelineRender(){if(!timelineFrame)timelineFrame=requestAnimationFrame(()=>{timelineFrame=0;renderEvents()})}
function openEvents(){
  if(!conversation)return;
  if(source)source.close();
  const active=conversation,stream=new EventSource(`/api/conversations/${conversation}/events?after=${lastEvent}`);
  source=stream;
  const current=()=>active===conversation&&source===stream;
  stream.onopen=()=>{if(current())$('#connection').textContent='实时连接'};
  stream.onerror=()=>{if(current())$('#connection').textContent='正在重连 · 业务继续执行'};
  stream.addEventListener('trace',event=>{
    if(!current())return;
    const data=JSON.parse(event.data);
    if(events.has(data.id))return;
    events.set(data.id,data);lastEvent=Math.max(lastEvent,data.id);
    scheduleTimelineRender();refresh({immediate:false}).catch(error);
  });
  stream.addEventListener('stream_closed',event=>{
    if(!current())return;
    const data=JSON.parse(event.data);
    stream.close();source=null;refresher.reset();lastSnapshot=null;
    if(data.error_code==='NOT_FOUND')conversation='';else csrf='';
    $('#connection').textContent='连接已结束';$('#cancel').disabled=true;
    $('#preview').replaceChildren(node('div','请重新进入工作台或新建会话。','blank-card'));
    error(Error(data.error_code==='NOT_FOUND'?'当前会话已不可用，请新建会话。':'会话已失效，请重新进入工作台。'));
  });
}
function renderEvents(){const elements=[...events.values()].map(e=>{const el=node('div',undefined,'event');const title=e.event_type==='user_notice'?'业务告知':e.event_type==='runtime_state'?'任务状态':e.event_type==='tool_result'?(tools[e.payload.tool]||e.payload.tool):kinds[e.event_type]||'处理记录';const top=node('div',title,'event-title');const time=node('time',new Date(e.created_at).toLocaleTimeString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false}));top.append(time);el.append(top);const details=node('details');let summary='查看记录';if(e.payload.result?.error_code)summary=e.payload.result.error_code;else if(e.event_type==='tool_result')summary=e.payload.result.ok?'处理成功':'处理受限';details.append(node('summary',summary),node('pre',JSON.stringify(e.payload,null,2)));if(developerMode)el.append(details);return el});$('#timeline').replaceChildren(...elements);$('#event-count').textContent=events.size+' 个事件';$('#timeline').scrollTop=$('#timeline').scrollHeight;}
async function refresh(options={}){if(!conversation)return;return refresher.request(conversation,options)}
function renderSnapshot(snap){lastSnapshot=snap;$('#status').textContent=labels[snap.task_state||snap.status]||snap.task_state||snap.status;const messages=snap.messages.map(m=>{const box=node('div',undefined,'bubble '+m.role);box.append(node('small',m.role==='user'?'你':'售后助手 · 脚本模拟'),node('div',m.text));return box});if(messages.length)$('#chat').replaceChildren(...messages);else $('#chat').replaceChildren(node('p','可以直接说明订单和诉求，例如“S1002 换大一码”。','placeholder'));$('#chat').scrollTop=$('#chat').scrollHeight;const p=snap.previews.find(p=>p.confirmation_available);renderPreview(p);$('#receipts').replaceChildren(...snap.operations.map(op=>{const el=node('div',undefined,'receipt');el.append(node('strong',labels[op.result]||op.result),node('div',op.operation_id),node('div',op.notice));return el}));$('#cancel').disabled=!snap.jobs.some(j=>['PENDING','RUNNING'].includes(j.status));}
function renderPreview(p){const container=$('#preview');if(!p){container.replaceChildren(node('div','尚无待确认操作','blank-card'));return}const data=p.payload,computed=data.computed;const card=node('div',undefined,'preview-card');card.append(node('h3',({cancel:'取消整单',return:'申请退货',exchange:'申请换货'})[data.action]));const dl=node('dl');for(const [k,v] of [['订单',data.order_id],['商品行',data.line_id||'整单'],['数量',data.action==='cancel'?'整单':data.quantity],['目标变体',p.target_label||'—'],['模拟运费',`¥${(computed.shipping_fee_fen/100).toFixed(2)}`],['有效期',new Date(p.expires_at).toLocaleTimeString('zh-CN',{timeZone:'Asia/Shanghai'})]]){dl.append(node('dt',k),node('dd',String(v)))}card.append(dl,node('p',computed.notice));const buttons=node('div',undefined,'actions');for(const [decision,label,cls] of [['reject','拒绝方案','reject'],['accept','确认并提交','accept']]){const b=node('button',p.status==='CONFIRMED'?'已确认 · 处理中':label,cls);b.disabled=p.status==='CONFIRMED';b.onclick=async()=>{b.disabled=true;try{await api(`/api/previews/${p.id}/confirm`,{method:'POST',body:JSON.stringify({decision})});await refresh()}catch(e){error(e);b.disabled=false}};buttons.append(b)}card.append(buttons);container.replaceChildren(card)}
async function send(event){event?.preventDefault();if(!csrf){error(Error('请先进入演示客户工作台'));return}if(!conversation)await newConversation();const text=$('#message').value.trim();if(!text)return;const id=crypto.randomUUID();$('#send').disabled=true;$('#error').hidden=true;try{await api(`/api/conversations/${conversation}/messages`,{method:'POST',body:JSON.stringify({text,client_message_id:id})});$('#message').value='';await refresh()}catch(e){error(e)}finally{$('#send').disabled=false}}
$('#login').onclick=login;$('#new-conversation').onclick=newConversation;$('#conversation').onchange=e=>selectConversation(e.target.value);$('#message-form').onsubmit=send;$('#message').onkeydown=e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();send()}};document.querySelectorAll('[data-prompt]').forEach(b=>b.onclick=()=>{$('#message').value=b.dataset.prompt;$('#message').focus()});$('#reconnect').onclick=openEvents;$('#cancel').onclick=async()=>{try{for(const job of lastSnapshot?.jobs||[])if(['RUNNING','PENDING'].includes(job.status))await api(`/api/jobs/${job.id}/cancel`,{method:'POST'});await refresh()}catch(e){error(e)}};document.querySelectorAll('[data-fault]').forEach(b=>b.onclick=async()=>{try{await api('/api/dev/faults',{method:'POST',body:JSON.stringify({conversation_id:conversation,fault:b.dataset.fault})});b.textContent='已设置 · '+b.textContent}catch(e){error(e)}});
(async()=>{try{const me=await api('/api/session');csrf=me.csrf_token;$('#customer').value=me.principal;await loadOrders();const list=await api('/api/conversations');const saved=localStorage.getItem('aftercare-conversation');conversation=list.some(c=>c.id===saved)?saved:list.at(-1)?.id||'';await loadConversations();if(conversation){connect();await refresh()}}catch{}const config=await fetch('/health/live').then(r=>r.json()).catch(()=>({}));developerMode=Boolean(config.demo_faults);$('#demo-controls').hidden=!developerMode;renderEvents();})();
