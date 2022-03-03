#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

class DBFile {

	private:
		File *fBuffer;	
		Page *wBuffer; 
		Page *rBuffer;	
		bool isEmptyWBuffer;	
		bool endOfFile;	
		int pageIndex;	

	public:
		DBFile (); 
		int Create (const char *fpath, fType file_type, void *startup);
		
		int Open (const char *fpath);

		int Close ();

		void Load (Schema &myschema, const char *loadpath);
		
		void MoveFirst ();
		
		void Add (Record &addme);
		
		int GetNext (Record &fetchme);
		
		int GetNext (Record &fetchme, CNF &cnf, Record &literal);
		
		int getSize();
};

#endif
