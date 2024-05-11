import entrypoints_config as cfg
from prefect.infrastructure import Process
from flows.common.dataplatform.deploy_utils import save_block, bash

queue_and_blocks_name = "ubuntu"
work_pool = "local-agent"
ib = f"-ib process/{queue_and_blocks_name}"
build = "prefect deployment build"
wq = f"-q {queue_and_blocks_name}"
wq = f"-p {work_pool}"
sb = "-sb gcs/flow-storage"
o = "flows/deployments/"
upload = "--skip-upload"

if __name__ == "__main__":
    # bash("python flows/common/dataplatform/utils/create_blocks.py")

    process_block = Process(env={"PREFECT_LOGGING_LEVEL": "DEBUG"})
    save_block(process_block, queue_and_blocks_name)

    # Deploy FLOWS
    for flow in cfg.bank_flows:
        flow_name = flow.split(':')[-1].replace("-","_")
        bash(f"{build} {ib} {sb} {wq} {wq} {flow} -n {flow_name} -o {o + flow_name + '.yaml'}")