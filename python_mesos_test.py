import uuid
import logging

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
logging.basicConfig(level=logging.INFO)

def new_task(offer):
    task = mesos_pb2.TaskInfo()
    id = uuid.uuid4()
    task.task_id.value = str(id)
    task.slave_id.value = offer.slave_id.value
    task.name = "task {}".format(str(id))

    cpus = task.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = 1

    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = 1

    return task


class TestScheduler(mesos.interface.Scheduler):

    def registered(self, driver, framework_id, master_info):
        print 'Registered with framework ID %s' % framework_id.value

    #offers는 마스터에 연결된 슬레이브 정보가 넘어온다.
    #슬레이브만큼 shell로 while문을 실행한다.
    #도커 및 슬레이브에 소스코드를 올린뒤 command로 실행 시킬 수 있다.
    def resourceOffers(self, driver, offers):
        for offer in offers:
            cpus = filter(lambda x: x.name == 'cpus', offer.resources)
            mem = filter(lambda x: x.name == 'mem', offer.resources)
            if cpus and mem:
                offer_cpus = cpus.pop().scalar.value
                offer_mem = mem.pop().scalar.value
                print 'Received offer %s with cpus: %s and mem: %s' % (offer.id.value, offer_cpus, offer_mem)
                task = new_task(offer)
                task.command.value = "while [ true ] ; do echo 'Hello Marathon' ; sleep 5 ; done"
                tasks = [task]
                driver.launchTasks(offer.id, tasks)


if __name__ == '__main__':
    framework = mesos_pb2.FrameworkInfo()
    framework.user = 'root'
    framework.name = 'python_test'
    framework.principal = 'test-framework-python'

    driver = mesos.native.MesosSchedulerDriver(
            TestScheduler(),
            framework,
                'zk://192.168.56.101:2181,192.168.56.102:2181,192.168.56.103:2181/mesos'
            )
    driver.run()
