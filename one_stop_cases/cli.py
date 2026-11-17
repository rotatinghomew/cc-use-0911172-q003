from __future__ import annotations
import argparse
import json
from pathlib import Path
from .reporting import Reporter
from .service import ApplicationService
from .cost.service import CostAnalysisService
from .cost.reporting import GROUP_VERSION_BATCH
from .storage import Database

def load_json(path:str):
    return json.loads(Path(path).read_text(encoding='utf-8'))

def main(argv=None):
    parser=argparse.ArgumentParser(prog='one_stop_cases')
    parser.add_argument('--database',default=':memory:')
    sub=parser.add_subparsers(dest='command',required=True)
    run=sub.add_parser('run'); run.add_argument('--fixture',required=True); run.add_argument('--output',required=True)
    report=sub.add_parser('report'); report.add_argument('--kind')
    cost=sub.add_parser('cost',help='actual handling cost analysis by service version and intake batch')
    cost.add_argument('--fixture',help='optional fixture providing cost events and workflows')
    cost.add_argument('--output')
    cost.add_argument('--group',default=GROUP_VERSION_BATCH,choices=('service_version','intake_batch_id','service_version|intake_batch_id','closure'))
    args=parser.parse_args(argv); database=Database(args.database)
    if args.command=='run':
        fixture=load_json(args.fixture); service=ApplicationService(database); outputs=[]
        for index,item in enumerate(fixture.get('items',[])):
            outputs.append(service.process(item,fixture.get('context',{}),item.get('request_key',f'item-{index}')))
        payload={'product':'人才服务一网通办事项编排与补偿平台','processed':len(outputs),'outputs':outputs,'report':Reporter(database).overview()}
        Path(args.output).parent.mkdir(parents=True,exist_ok=True); Path(args.output).write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8')
        print(json.dumps(payload,ensure_ascii=False,sort_keys=True)); return 0
    if args.command=='cost':
        from .cost.events import event_from_mapping
        workflows={}; raw_events=[]
        if args.fixture:
            fixture=load_json(args.fixture)
            workflows=fixture.get('workflows',{})
            raw_events=fixture.get('cost_events',fixture.get('events',[]))
        service=CostAnalysisService(database,workflows=workflows)
        events,problems=service.collect_events()
        if raw_events:
            extra=tuple(event_from_mapping(row,index+1,source='fixture') for index,row in enumerate(raw_events))
            seen={event.event_id:event for event in events}
            for event in extra: seen.setdefault(event.event_id,event)
            events=tuple(seen.values())
        cases=service.analyzer.analyze(events,problems)
        payload=service.reporter.report(cases,args.group)
        payload['evidence_problems']=sorted(service.analyzer.global_evidence_problems)
        text=json.dumps(payload,ensure_ascii=False,indent=2,sort_keys=True)
        if args.output:
            Path(args.output).parent.mkdir(parents=True,exist_ok=True); Path(args.output).write_text(text,encoding='utf-8')
        print(text); return 0
    payload=Reporter(database).owner_breakdown(args.kind) if args.kind else Reporter(database).overview(); print(json.dumps(payload,ensure_ascii=False,sort_keys=True)); return 0
