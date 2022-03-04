#include <vector>
#include <queue>
#include <algorithm>

#include "BigQ.h"



struct PriorityQueue_Record {
	Record *currentRecord;
	int pageNumber;
	int bufferNumber;
};


class PriorityQueue_Comparator {
	private:
		OrderMaker *sortOrderPointer;
		ComparisonEngine compEngine;

	public:
		PriorityQueue_Comparator(OrderMaker *sortOrder) {
			this -> sortOrderPointer = sortOrder;
		}

		bool operator()(PriorityQueue_Record *recordOne, PriorityQueue_Record *recordTwo) {
			if (compEngine.Compare(recordOne -> currentRecord, recordTwo -> currentRecord, sortOrderPointer) > 0)
				return true;

			return false;
		}
};


class RecordComparator {
	private:
		OrderMaker *sortOrderPointer;
		ComparisonEngine compEngine;

	public:
		RecordComparator(OrderMaker *sortOrderPointer) {
			this -> sortOrderPointer = sortOrderPointer;
		}

		bool operator()(Record *recordOne, Record *recordTwo) {
			if (compEngine.Compare(recordOne, recordTwo, sortOrderPointer) < 0)
				return true;

			return false;
		}
};


BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	this -> inPipe = &in;
	this -> outPipe = &out;
	this -> sortOrderPointer = &sortorder;
	this -> runLen = &runlen;

	this -> file = new File();
	this -> file -> Open(0, "runs.bin");
	this -> file -> Close();
	this -> file -> Open(1, "runs.bin");

	pthread_t workerThread;
	pthread_create(&workerThread, NULL, BigQ::invokeTPMMSAlgo, (void *)this);
	pthread_join(workerThread, NULL);

	out.ShutDown();
}


void *BigQ::invokeTPMMSAlgo(void *args) {
	((BigQ *)args) -> workerMethod();

	return nullptr;
}


void BigQ::workerMethod() {

	int runlen = *runLen;

	int pagesCurrentlyExecuting = 0;

	Page *bufferPage = new Page();

	Record *currentRecord = new Record();

	runPointersList.push_back(0);
	
	vector<Record *> currentExecutingVector;
	
	int numberOfRuns = 0;

	Record *temporaryRecord = new Record();

	while (inPipe -> Remove(currentRecord)) {

		if (pagesCurrentlyExecuting < runlen) {

			int res = bufferPage -> Append(currentRecord);
			if (res == 0) {
				pagesCurrentlyExecuting++;

				while (bufferPage -> GetFirst(temporaryRecord)) {
					currentExecutingVector.push_back(temporaryRecord);
					temporaryRecord = new Record();
				}

				bufferPage -> EmptyItOut();
				bufferPage -> Append(currentRecord);
			}
		} else {
			pagesCurrentlyExecuting = 0;

			this -> sortRunMethod(currentExecutingVector);
			numberOfRuns++;
			
			int runhead = this -> addRunToFileMethod(currentExecutingVector);

			this -> runPointersList.push_back(runhead);
			currentExecutingVector.clear();
			bufferPage -> Append(currentRecord);
		}
	}

	numberOfRuns++;
	while (bufferPage -> GetFirst(temporaryRecord)) {
		currentExecutingVector.push_back(temporaryRecord);
		temporaryRecord = new Record();
	}

	this -> sortRunMethod(currentExecutingVector);
	int executeHead = this -> addRunToFileMethod(currentExecutingVector);
	this -> runPointersList.push_back(executeHead);
	currentExecutingVector.clear();
	this -> file -> Close();

	this -> file = new File();
	file -> Open(1, "runs.bin");
	typedef priority_queue<PriorityQueue_Record *, std::vector<PriorityQueue_Record *>, PriorityQueue_Comparator> priorityQueue_merger_type;
	priorityQueue_merger_type priorityQueue_Merger(sortOrderPointer);
	
	Page *runBuffers[numberOfRuns];

	PriorityQueue_Record *priorityQueue_Record = new PriorityQueue_Record();
	currentRecord = new Record();

	int count = 0;
	while (count < numberOfRuns) {
		runBuffers[count] = new Page();
		file -> GetPage(runBuffers[count], this -> runPointersList[count]);
		runBuffers[count] -> GetFirst(currentRecord);
		priorityQueue_Record -> currentRecord = currentRecord;
		priorityQueue_Record -> pageNumber = this -> runPointersList[count];
		priorityQueue_Record -> bufferNumber = count;
		priorityQueue_Merger.push(priorityQueue_Record);
		priorityQueue_Record = new PriorityQueue_Record();
		currentRecord = new Record();
		count++;
	}

	while (!priorityQueue_Merger.empty()) {
		priorityQueue_Record = priorityQueue_Merger.top();
		int pageNumber = priorityQueue_Record -> pageNumber;
		int bufferNumber = priorityQueue_Record -> bufferNumber;
		this -> outPipe -> Insert(priorityQueue_Record -> currentRecord);
		priorityQueue_Merger.pop();

		Record *record = new Record();
		if (runBuffers[bufferNumber] -> GetFirst(record) == 0) {

			pageNumber = pageNumber + 1;
			if (pageNumber < (file -> GetLength() - 1) && (pageNumber < this -> runPointersList[bufferNumber + 1])) {
				runBuffers[bufferNumber] -> EmptyItOut();
				file -> GetPage(runBuffers[bufferNumber], pageNumber);

				if (runBuffers[bufferNumber] -> GetFirst(record) != 0) {
					priorityQueue_Record -> currentRecord = record;
					priorityQueue_Record -> bufferNumber = bufferNumber;
					priorityQueue_Record -> pageNumber = pageNumber;
					priorityQueue_Merger.push(priorityQueue_Record);
				}
			}
		} else {
			priorityQueue_Record -> currentRecord = record;
			priorityQueue_Record -> bufferNumber = bufferNumber;
			priorityQueue_Record -> pageNumber = pageNumber;
			priorityQueue_Merger.push(priorityQueue_Record);
		}
	}

	file -> Close();
}

int BigQ::addRunToFileMethod(vector<Record *> &vector) {
	Page *bufferPage = new Page();

	for (int i = 0; i < vector.size(); i++) {
		if (bufferPage -> Append(vector[i]) == 0) {
			if (this -> file -> GetLength() == 0) {
				this -> file -> AddPage(bufferPage, 0);
			} else {
				this -> file -> AddPage(bufferPage, this -> file -> GetLength() - 1);
			}
			bufferPage -> EmptyItOut();
			bufferPage -> Append(vector[i]);
		}
	}

	if (this -> file -> GetLength() == 0) {
		this -> file -> AddPage(bufferPage, 0);
	} else {
		this -> file -> AddPage(bufferPage, this -> file -> GetLength() - 1);
	}

	return this -> file -> GetLength() - 1; 
}

void BigQ::sortRunMethod(vector<Record *> &vector) {
	sort(vector.begin(), vector.end(), RecordComparator(sortOrderPointer));
}


BigQ::~BigQ()
{
	this -> outPipe -> ShutDown();
}