package main

import (
	"fmt"
)

type task struct {
	id      string
	memNeed int
}

type worker struct {
	workerID string
	memory   int
}

type taskGroup struct {
	unAllocTasks []task //尚未分配执行的任务，按memNeed升序排列
}

type sche struct {
	freeWorkers []worker //根据memory按升序排列
}

func (s *sche) tryDispatchJob(group *taskGroup) {
	workerCount := len(s.freeWorkers)
	taskCount := len(group.unAllocTasks)
	workerIdx := 0
	taskIdx := 0

	markRemove := make([]bool, len(s.freeWorkers))

	for workerCount > 0 && taskCount > 0 {
		lower := group.unAllocTasks[taskIdx].memNeed

		upper := lower
		if taskCount > 1 {
			upper += group.unAllocTasks[taskIdx+1].memNeed
		}

		lowerIdx := -1 //满足lower内存要求的第一个worker在freeWorkers中的下标
		upperIdx := -1 //满足upper内存要求的第一个worker在freeWorkers中的下标
		for i := workerIdx; i < len(s.freeWorkers); i++ {
			v := s.freeWorkers[i]
			if lowerIdx == -1 && v.memory >= lower {
				lowerIdx = i
			}

			if upperIdx == -1 && v.memory >= upper {
				upperIdx = i
			}

			if lowerIdx >= 0 && upperIdx >= 0 {
				break
			}
		}

		if lowerIdx == -1 {
			break
		}

		idx := lowerIdx

		tasks := []task{group.unAllocTasks[taskIdx]}
		taskCount--
		taskIdx++
		if lower != upper && upperIdx >= 0 && s.freeWorkers[upperIdx].memory >= upper {
			tasks = append(tasks, group.unAllocTasks[taskIdx])
			taskCount--
			taskIdx++
			idx = upperIdx
		}

		workerCount--
		markRemove[idx] = true
		workerIdx = idx + 1
		fmt.Println("worker", s.freeWorkers[idx], "tasks", tasks)
	}

	//将removeWorker中标记的worker移除
	var freeWorkers []worker
	for i, v := range s.freeWorkers {
		if !markRemove[i] {
			freeWorkers = append(freeWorkers, v)
		}
	}
	s.freeWorkers = freeWorkers

	//将taskIdx之前的task移除
	for i := taskIdx; i < len(group.unAllocTasks); i++ {
		group.unAllocTasks[i-taskIdx] = group.unAllocTasks[i]
	}
	group.unAllocTasks = group.unAllocTasks[:len(group.unAllocTasks)-taskIdx]

}

func main() {
	{
		fmt.Println("test1-------------")
		sc := &sche{}

		sc.freeWorkers = append(sc.freeWorkers, worker{
			workerID: "worker1",
			memory:   4,
		})

		sc.freeWorkers = append(sc.freeWorkers, worker{
			workerID: "worker2",
			memory:   8,
		})

		sc.freeWorkers = append(sc.freeWorkers, worker{
			workerID: "worker3",
			memory:   12,
		})

		group1 := &taskGroup{}
		group1.unAllocTasks = append(group1.unAllocTasks, task{
			id:      "task1",
			memNeed: 1,
		})
		group1.unAllocTasks = append(group1.unAllocTasks, task{
			id:      "task2",
			memNeed: 3,
		})
		group1.unAllocTasks = append(group1.unAllocTasks, task{
			id:      "task3",
			memNeed: 9,
		})
		group1.unAllocTasks = append(group1.unAllocTasks, task{
			id:      "task4",
			memNeed: 9,
		})

		sc.tryDispatchJob(group1)

		fmt.Println("unAllocTasks", group1.unAllocTasks, "freeWorkers", sc.freeWorkers)

	}

}
