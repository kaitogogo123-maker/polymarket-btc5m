"""Full simulated-order flow on the public page with real input, through a real settlement, checked against the official result.
Takes up to 10 minutes. Run: python tests/e2e_paper.py   Headless; sends no real orders."""
import json,subprocess,time,requests,websockets,asyncio,os,sys,base64
import tempfile
SP=tempfile.mkdtemp()
URL="https://polymarket-btc5m.kaitogogo123.workers.dev/"
C="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PORT=9353
p=subprocess.Popen([C,"--headless=new","--use-gl=swiftshader","--disable-gpu",f"--remote-debugging-port={PORT}","--user-data-dir="+SP+f"/cdp_flow{int(time.time())}","--disable-background-timer-throttling","about:blank"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
for _ in range(40):
    try:
        tab=[t for t in requests.get(f"http://127.0.0.1:{PORT}/json",timeout=1).json() if t["type"]=="page"][0];break
    except Exception: time.sleep(.5)
fails=[]
def check(name,ok,info=''):
    print(('通过 ' if ok else '失败 ')+name,info,flush=True)
    if not ok: fails.append(name)
def fee(p): return .07*p*(1-p)
def walk(levels,amount):
    left,sh,f=amount,0,0
    for p,s in levels:
        if not s or left<=1e-9: continue
        t=min(left/p,s);sh+=t;f+=t*fee(p);left-=t*p
    return sh,amount-left,f
async def main():
  async with websockets.connect(tab["webSocketDebuggerUrl"],max_size=2**26,ping_interval=None) as w:
    i=0;errs=[]
    async def cmd(m,**k):
        nonlocal i;i+=1;me=i
        await w.send(json.dumps(dict(id=me,method=m,params=k)))
        while True:
            r=json.loads(await asyncio.wait_for(w.recv(),30))
            if r.get('method')=='Runtime.exceptionThrown':errs.append(r['params']['exceptionDetails'].get('exception',{}).get('description','')[:200])
            if r.get("id")==me:return r.get("result",r)
    async def ev(x):
        r=await cmd("Runtime.evaluate",expression=x,returnByValue=True,awaitPromise=True);return r.get('result',{}).get('value',r)
    async def shot(n):
        r=await cmd("Page.captureScreenshot",format="png");open(f"{SP}/h_{n}.png","wb").write(base64.b64decode(r["data"]))
    async def pos(sel): return await ev(f"(()=>{{const e=document.querySelector('{sel}');e.scrollIntoView({{block:'center'}});const r=e.getBoundingClientRect();return [r.left+r.width/2,r.top+r.height/2]}})()")
    async def click(sel):
        c=await pos(sel)
        for t,k in (("mouseMoved",{}),("mousePressed",dict(button="left",clickCount=1)),("mouseReleased",dict(button="left",clickCount=1))): await cmd("Input.dispatchMouseEvent",type=t,x=c[0],y=c[1],**k)
    async def tap(sel):
        c=await pos(sel)
        await cmd("Input.dispatchTouchEvent",type="touchStart",touchPoints=[dict(x=c[0],y=c[1])]);await asyncio.sleep(.05);await cmd("Input.dispatchTouchEvent",type="touchEnd",touchPoints=[])
    async def typ(v):
        await click('#amt');await ev("$('amt').select()");await cmd("Input.dispatchKeyEvent",type="keyDown",key="Backspace",code="Backspace",windowsVirtualKeyCode=8);await cmd("Input.dispatchKeyEvent",type="keyUp",key="Backspace",code="Backspace",windowsVirtualKeyCode=8)
        if v: await cmd("Input.insertText",text=v)
        await asyncio.sleep(.7)
        return await ev("[$('amt').value,$('place').textContent,$('place').disabled]")
    orders=lambda: ev("JSON.stringify(paper)")
    async def paper(): return json.loads(await orders())
    await cmd("Runtime.enable");await cmd("Network.enable");await cmd("Page.navigate",url=URL)
    await cmd("Emulation.setDeviceMetricsOverride",width=1440,height=900,deviceScaleFactor=2,mobile=False);await asyncio.sleep(7)
    # wait for time to do things: at least 90 s left in the window
    while (await ev("S.T+300-Date.now()/1000"))<100: await asyncio.sleep(2)
    print('窗口',await ev("S.T"),'剩',round(await ev("S.T+300-Date.now()/1000")),'s',flush=True)
    # A inputs
    for v,want_dis in (('',True),('0',True),('0.5',True),('abc',True),('-5',None),('1e5',None),('12.345',False),('1.2.3',None),('999999',True)):
        r=await typ(v);print('  输入',repr(v),r,flush=True)
        if want_dis is not None: check(f'输入 {v!r} 按钮{"禁用" if want_dis else "可点"}',r[2]==want_dis,r[1])
    check('非法字符被过滤',(await typ('abc'))[0]=='')
    check('小数保留两位',(await typ('12.345'))[0]=='12.34')
    check('输入过程没有产生订单',len(await paper())==0)
    # B place Up by mouse: independent arithmetic
    await typ('10');await click('#upb');await asyncio.sleep(.6)
    await click('#place');L=await ev("JSON.stringify(S.ladder)");await asyncio.sleep(.5);o=(await paper())[0]
    L=json.loads(L);sh,sp,f=walk(L['asks'],10)
    check('Up 成交按卖盘计算',abs(o['spent']-10)<.01 and o['side']=='up' and o['shares']>0 and 0.005<o['avg']<.995,o)
    best=L['asks'][0][0]
    check('Up 均价不优于卖一太多',o['avg']>=best-.03,f"avg {o['avg']:.3f} best {best}")
    check('Up 手续费公式',abs(o['fee']-sum(0 for _ in [0])-o['shares']*fee(o['avg']))<o['shares']*.002,o['fee'])
    check('下单后反馈 已记录',await ev("$('place').textContent")=='已记录')
    # C double click only one
    await asyncio.sleep(1.6);n0=len(await paper());c=await pos('#place')
    for _ in range(3):
        await cmd("Input.dispatchMouseEvent",type="mousePressed",x=c[0],y=c[1],button="left",clickCount=1);await cmd("Input.dispatchMouseEvent",type="mouseReleased",x=c[0],y=c[1],button="left",clickCount=1)
    await asyncio.sleep(.5);check('连点三下只记一笔',len(await paper())==n0+1,len(await paper())-n0)
    # D Enter key on amount
    await asyncio.sleep(1.6);n0=len(await paper());await click('#amt');await cmd("Input.dispatchKeyEvent",type="keyDown",key="Enter",code="Enter",windowsVirtualKeyCode=13,text="\r");await cmd("Input.dispatchKeyEvent",type="keyUp",key="Enter",code="Enter",windowsVirtualKeyCode=13);await asyncio.sleep(.5)
    check('金额框按回车下单',len(await paper())==n0+1)
    # E phone with real touch: Down
    await cmd("Emulation.setDeviceMetricsOverride",width=390,height=844,deviceScaleFactor=2,mobile=True);await cmd("Emulation.setTouchEmulationEnabled",enabled=True);await asyncio.sleep(1.6)
    n0=len(await paper());await tap('#downb');await asyncio.sleep(.6);await tap('.add[data-a="10"]');await asyncio.sleep(.6)
    amt=await ev("$('amt').value");await tap('#place');L=json.loads(await ev("JSON.stringify(S.ladder)"));await asyncio.sleep(.7);ps=await paper()
    check('手机触摸下 Down',len(ps)==n0+1 and ps[0]['side']=='down',ps[0]);check('手机金额 +10',abs(ps[0]['spent']-float(amt))<.01,amt)
    check('Down 价约等于 1-买一',ps[0]['avg']>=1-L['bids'][0][0]-.03,f"{ps[0]['avg']:.3f} vs {1-L['bids'][0][0]:.3f}")
    check('手机无横向滚动',await ev("document.documentElement.scrollWidth<=innerWidth"))
    check('手机输入框 16px 不触发缩放',await ev("getComputedStyle($('amt')).fontSize")=='16px')
    await shot('phone_after_order')
    await cmd("Emulation.setTouchEmulationEnabled",enabled=False);await cmd("Emulation.setDeviceMetricsOverride",width=1440,height=900,deviceScaleFactor=2,mobile=False);await asyncio.sleep(.8)
    await shot('desk_after_order')
    # F reload keeps records
    before=await paper();await cmd("Page.reload");await asyncio.sleep(6);after=await paper()
    check('刷新后记录还在',before==after,len(after));check('刷新后仍显示记录',str(len(after))+' 笔' in await ev("$('papers').textContent"))
    # G past window and back
    await click('#balls button');await asyncio.sleep(1.5);check('看历史窗口时隐藏下单和记录',await ev("$('trade').hidden&&$('paper').hidden"))
    await click('#golive');await asyncio.sleep(1.5);check('回到实时后恢复',await ev("!$('trade').hidden&&!$('paper').hidden&&S.view===S.T"))
    # H real offline: no order on stale book, no alarm, recovers
    left=await ev("S.T+300-Date.now()/1000")
    if left>60:
        await cmd("Network.emulateNetworkConditions",offline=True,latency=0,downloadThroughput=-1,uploadThroughput=-1);await asyncio.sleep(33)
        r=await ev("[$('place').disabled,$('place').textContent,$('status').textContent.slice(-8),$('alert').hidden,document.body.className,document.title]");print('  断网 33s',r,flush=True)
        check('断网过久按钮禁用',r[0] and r[1]=='等待最新盘口',r[1]);check('断网不出红条',r[3]);check('断网不出现断开字样','断开' not in await ev("document.body.innerText+document.title"))
        await cmd("Network.emulateNetworkConditions",offline=False,latency=0,downloadThroughput=-1,uploadThroughput=-1);await asyncio.sleep(6)
        check('恢复后不再等盘口',await ev("$('place').textContent")!='等待最新盘口',await ev("$('place').textContent"))
    else: print('  断网测试跳过: 窗口只剩',round(left),flush=True)
    # I last 5 seconds
    T=await ev("S.T")
    while (await ev("Date.now()/1000"))<T+297: await asyncio.sleep(.5)
    r=await ev("[$('place').disabled,$('place').textContent]");check('最后 5 秒不能下',r[0] and r[1]=='即将结算',r)
    await shot('last5')
    # J settlement
    t0=time.time();rows=[]
    while time.time()-t0<120:
        ps=await paper();rows=[o for o in ps if o['T']==T]
        if rows and all(o['win'] is not None for o in rows): break
        await asyncio.sleep(1)
    print('  收盘后结算用时',round(await ev("Date.now()/1000")-(T+300)),'s',flush=True)
    check('到期都结算了',rows and all(o['win'] is not None for o in rows))
    await asyncio.sleep(1);text=await ev("[$('papers').textContent,$('paperl').textContent]");print('  显示',text,flush=True);await shot('settled')
    off=None
    for _ in range(40):
        g=requests.get('https://gamma-api.polymarket.com/markets',params={'slug':f'btc-updown-5m-{T}','closed':'true'},timeout=8).json()
        if g:
            pr=json.loads(g[0]['outcomePrices'])
            if pr[0] in('0','1'): off=pr[0]=='1';break
        await asyncio.sleep(15)
    print('  官方结果 Up' if off else '  官方结果 Down',T,flush=True)
    check('拿到官方结果',off is not None)
    if off is not None:
        for o in rows: check(f"{o['side']} 输赢与官方一致",o['win']==(off==(o['side']=='up')),o['win'])
        tot=sum((o['shares'] if o['win'] else 0)-o['spent']-o['fee'] for o in rows)
        print('  手算累计',round(tot,2),flush=True)
        check('累计盈亏等于手算',f"${abs(tot):.2f}" in text[0] and (('−' in text[0])==(tot<0)),text[0])
    # K new window trades on the new book
    await asyncio.sleep(3);T2=await ev("S.T");check('自动进入新窗口',T2==int(time.time())//300*300 and T2>T,T2)
    n0=len(await paper());await asyncio.sleep(1.6);await click('#upb');await asyncio.sleep(.5);await click('#place');await asyncio.sleep(.6);ps=await paper()
    check('新窗口能下单且归新窗口',len(ps)==n0+1 and ps[0]['T']==T2,ps[0]['T'])
    # L corrupted storage does not break the page
    await ev("localStorage.setItem('paper','{bad')");await cmd("Page.reload");await asyncio.sleep(6)
    check('本地数据损坏页面仍正常',await ev("!!S&&$('paper').hidden&&!$('place').disabled"),await ev("$('place').textContent"))
    check('全程无 JS 错误',not errs,errs[:3])
  print('失败项',fails or '无',flush=True)
asyncio.run(main());p.terminate()
