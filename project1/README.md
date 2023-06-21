# 数据收集项目




    ------------------     ------------------
    |  worker node   |     |  worker node    | 
    |                |     |                 |
    |worker   worker |     |worker    worker | ...  more worker nodes      
    |    |     |     |     |    |      |     |
    |     agent      |     |     agent       |
    ------------------     -------------------
              \               / 
               \             /
                \           /
                  scheduler
                      |
                      |
                      |
                     user   


* scheduler:任务接口及调度器，接受来自user的任务，将任务调度到worker node执行并保存任务计算的结果。
* agent:worker node代理进程，负责与scheduler通信接受任务，将任务交给worker处理，将计算结果返回scheduler，并定时与scheduler保持心跳。
* worker:计算进程，从agent接受计算任务，将计算结果返回agent之后进程结束。  



## 任务的执行

user向scheduler上传任务列表。scheduler调度agent处理任务。

### 计算资源的分配

根据任务需要的内存大小，从可用资源池中选择内存匹配的节点（如果无可用节点则暂时不分配任务，待有节点完成计算任务归还资源池再分配）。


任务被分配后记录任务所在节点，如果结算节点发生故障（心跳超时）需要将任务重置为待计算状态，重新分配执行。

计算节点定期通过心跳向scheduler上报计算进度。计算完成后将结果上报scheduler。

结果被scheduler接受后,节点重置状态，向scheduler报告空闲，scheduler将节点再次投入到资源池。





















