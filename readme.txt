人才服务一网通办事项编排与补偿平台
测试：python3 -m unittest discover -s tests -v
构建：python3 -m compileall -q one_stop_cases tests
验收：python3 -m one_stop_cases --database /tmp/one_stop_cases.sqlite3 run --fixture fixtures/acceptance.json --output /tmp/one_stop_cases-output.json
单测：python3 -m unittest tests.test_domain -v

实际办理成本分析（只读，不改变可执行步骤与补偿计划）：
  python3 -m one_stop_cases --database /tmp/cost.sqlite3 cost \
      --fixture fixtures/cost_rework.json \
      --group 'service_version|intake_batch_id' \
      --output /tmp/cost-output.json
  分组维度：service_version / intake_batch_id / service_version|intake_batch_id / closure
说明：
  - 工作量区分 first_execution（首次办理）、compensation（失败后补偿）、re_execution（补偿后再次执行），
    每条工作量都回链到同一受理案、具体步骤与可回查的事件证据。
  - 受理量按受理案去重，幂等重试计入 idempotent_replay_count，不放大受理量。
  - effort_seconds 为各步骤有效工时之和；并行步骤的 active_seconds 按时间区间并集计算，不重复累加等待。
  - 未办结案件标记 unclosed，缺少关键事件的案件标记 insufficient_evidence，均不计为零成本或正常办结。
  - grouped_summary 与 work_details/case_details 通过 reconciliation 双向对账。
