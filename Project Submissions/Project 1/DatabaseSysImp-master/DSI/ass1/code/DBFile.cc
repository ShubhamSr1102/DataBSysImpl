#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"


DBFile::DBFile () {

	pointerCurrentRecord = new Record();
	allRecordsRead = false;

	presentPageNumber = 0;
	fM = WRITE;

	pageNumberRead = 0;
	pageRecordNumberRead = 0;

}


int DBFile::Create (const char *f_path, fType f_type, void *startup) {
	
	if (f_type == heap) {
    	file.Open(0, (char *)f_path);	
    	return 1;
	} else if (f_type == sorted) {
		cout << "ImplementationError: SortedFile" << endl;
	} else if (f_type == tree){
		cout << "ImplementationError: TreeFile" << endl;
	} else {
		cout << "UndefinedError: No such implementation" << endl;
	}
	return 0;
}


void DBFile::Load (Schema &f_schema, const char *loadpath) {

	WriteModeSwitch();
	FILE *fileTable = fopen (loadpath, "r");	
	Record record;	
	while(record.SuckNextRecord(&f_schema, fileTable) == 1) {
		Add(record);	
	}
	ReadModeSwitch();
}


int DBFile::Open (const char *f_path) {

	file.Open(1, (char *)f_path);
	return 1;

}


void DBFile::MoveFirst () {

	allRecordsRead = false;
	pageNumberRead = 0;
	pageRecordNumberRead = 0;
	ReadModeSwitch();
	presentPageNumber = 0;	// Read from start.
	file.GetPage(&page, presentPageNumber);	// Fetch the first page.
	page.GetFirst(pointerCurrentRecord);	// Initialize the current pointer to the first record.

}


int DBFile::Close () {

	file.Close();
	return 1;

}


void DBFile::Add (Record &rec) {

	WriteModeSwitch();
	int status = page.Append(&rec);
	if (status == 0) {	
		file.AddPage(&page, presentPageNumber);	
		presentPageNumber += 1;	
		page.EmptyItOut();	
		page.Append(&rec);	
	}

}


int DBFile::GetNext (Record &fetchme) {

	if (allRecordsRead) {
		return 0;
	}
	fetchme.Consume(pointerCurrentRecord);

	if (page.GetFirst(pointerCurrentRecord) == 1) { 
		pageRecordNumberRead++;
		return 1;
	}

	presentPageNumber += 1; 
	if (presentPageNumber == file.GetLength()-1) {
		allRecordsRead = true;
		return 1;
	}

	file.GetPage(&page, presentPageNumber);	
	pageNumberRead += 1;
	if (page.GetFirst(pointerCurrentRecord) == 1) {	
		pageRecordNumberRead++;
		return 1;
	}

}


int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

	ComparisonEngine cmprEngine;

	while(GetNext(fetchme)) {
		if (cmprEngine.Compare(&fetchme, &literal, &cnf) == 1) {
			return 1;
		}
	}

	return 0; 

}


void DBFile::WriteModeSwitch() {

	if (fM == WRITE) {
		return;
	}
	fM = WRITE; 

	presentPageNumber = file.GetLength() - 2 < 0 ? 0 : file.GetLength() - 2; 
	file.GetPage(&page, presentPageNumber);	// Fetch the required page.

}


void DBFile::ReadModeSwitch() {

	if (fM == READ) {
		return;
	}

	fM = READ;	

	if (page.GetNumberRecords() != 0) {	
		file.AddPage(&page, presentPageNumber);	
	}
	file.GetPage(&page, pageNumberRead); 
	Record tempRecord;

	for (int i = 0; i < pageRecordNumberRead; i++) {	
		if (GetNext(tempRecord)) {
			pointerCurrentRecord = &tempRecord;
		}
	}

}
