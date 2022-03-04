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
		PriorityQueue_Comparator(OrderMaker *sortOrderPointer) {
			this -> sortOrderPointer = sortOrderPointer;
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
	this -> postSortOrder = &sortorder;
	this -> runLengthVar = &runlen;

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

	int runLen = *runLengthVar;

	int pagesExecuting = 0;

	Page *bufferPage = new Page();

	Record *currentRecord = new Record();

	runPointersList.push_back(0);
	
	vector<Record *> currentRunVector;
	
	int numberOfRuns = 0;

	Record *tempRecord = new Record();

	while (inPipe -> Remove(currentRecord)) {

		if (pagesExecuting < runLen) {

			int res = bufferPage -> Append(currentRecord);
			if (res == 0) {
				pagesExecuting++;

				while (bufferPage -> GetFirst(tempRecord)) {
					currentRunVector.push_back(tempRecord);
					tempRecord = new Record();
				}

				bufferPage -> EmptyItOut();
				bufferPage -> Append(currentRecord);
			}
		} else {
			pagesExecuting = 0;

			this -> sortRunMethod(currentRunVector);
			numberOfRuns++;
			
			int executeHead = this -> initiateFiletoRun(currentRunVector);

			this -> runPointersList.push_back(executeHead);
			currentRunVector.clear();
			bufferPage -> Append(currentRecord);
		}
	}

	numberOfRuns++;
	while (bufferPage -> GetFirst(tempRecord)) {
		currentRunVector.push_back(tempRecord);
		tempRecord = new Record();
	}

	this -> sortRunMethod(currentRunVector);
	int executeHead = this -> initiateFiletoRun(currentRunVector);
	this -> runPointersList.push_back(executeHead);
	currentRunVector.clear();
	this -> file -> Close();

	this -> file = new File();
	file -> Open(1, "runs.bin");
	typedef priority_queue<PriorityQueue_Record *, std::vector<PriorityQueue_Record *>, PriorityQueue_Comparator> priorityQueue_merger_type;
	priorityQueue_merger_type priorityQueue_merger(postSortOrder);
	

	Page *runPageBuffers[numberOfRuns];

	PriorityQueue_Record *priorityQueue_Record = new PriorityQueue_Record();
	currentRecord = new Record();

	int i = 0;

	while (i < numberOfRuns) {
		runPageBuffers[i] = new Page();
		file -> GetPage(runPageBuffers[i], this -> runPointersList[i]);
		runPageBuffers[i] -> GetFirst(currentRecord);
		priorityQueue_Record -> currentRecord = currentRecord;
		priorityQueue_Record -> pageNumber = this -> runPointersList[i];
		priorityQueue_Record -> bufferNumber = i;
		priorityQueue_merger.push(priorityQueue_Record);
		priorityQueue_Record = new PriorityQueue_Record();
		currentRecord = new Record();
		i++;
	}

	while (!priorityQueue_merger.empty()) {
		priorityQueue_Record = priorityQueue_merger.top();
		int pageNumber = priorityQueue_Record -> pageNumber;
		int bufferNumber = priorityQueue_Record -> bufferNumber;
		this -> outPipe -> Insert(priorityQueue_Record -> currentRecord);
		priorityQueue_merger.pop();

		Record *record = new Record();
		if (runPageBuffers[bufferNumber] -> GetFirst(record) == 0) {

			pageNumber = pageNumber + 1;
			if (pageNumber < (file -> GetLength() - 1) && (pageNumber < this -> runPointersList[bufferNumber + 1])) {
				runPageBuffers[bufferNumber] -> EmptyItOut();
				file -> GetPage(runPageBuffers[bufferNumber], pageNumber);

				if (runPageBuffers[bufferNumber] -> GetFirst(record) != 0) {
					priorityQueue_Record -> currentRecord = record;
					priorityQueue_Record -> bufferNumber = bufferNumber;
					priorityQueue_Record -> pageNumber = pageNumber;
					priorityQueue_merger.push(priorityQueue_Record);
				}
			}
		} else {
			priorityQueue_Record -> currentRecord = record;
			priorityQueue_Record -> bufferNumber = bufferNumber;
			priorityQueue_Record -> pageNumber = pageNumber;
			priorityQueue_merger.push(priorityQueue_Record);
		}
	}

	file -> Close();
}


int BigQ::initiateFiletoRun(vector<Record *> &vector) {
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
	sort(vector.begin(), vector.end(), RecordComparator(postSortOrder));
}


BigQ::~BigQ()
{
	this -> outPipe -> ShutDown();
}