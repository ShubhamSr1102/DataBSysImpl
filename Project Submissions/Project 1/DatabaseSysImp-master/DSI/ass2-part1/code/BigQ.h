#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>

#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;


class BigQ {

	private:
		Pipe *inPipe;	
		Pipe *outPipe;
		OrderMaker *postSortOrder;	
		int *runLengthVar;	
		File *file;	 
		vector<int> runPointersList;	

	public:
		BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);

		void workerMethod();
		
		static void* invokeTPMMSAlgo( void *args );
		
		void sortRunMethod( vector <Record*> &);
		
		int initiateFiletoRun(vector <Record*> & );

		~BigQ ();
};

#endif