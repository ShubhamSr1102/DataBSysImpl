#ifndef DBFILE_H
#define DBFILE_H

#include <iostream>

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

//using namespace std;

typedef enum {heap, sorted, tree} fType;
typedef enum {READ, WRITE} fMode;


class DBFile {

private:
	File file;
	Page page;

public:

	Record *pointerCurrentRecord;
	bool allRecordsRead;
	int presentPageNumber;	
	fMode fM;	

	int pageNumberRead;
	int pageRecordNumberRead;

	DBFile (); 

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	void ReadModeSwitch();
	void WriteModeSwitch();
	
};

#endif