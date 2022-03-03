#include <stdio.h>

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "string.h"
#include "fstream"


DBFile::DBFile () {
    this -> fBuffer = new File();
    this -> wBuffer = new Page(); 
    this -> rBuffer = new Page();
    this -> isEmptyWBuffer = true;
    this -> endOfFile = false;
    this -> pageIndex = 0;
}


int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    char meta_data[100];
    strcpy(meta_data , f_path);
    strcat(meta_data , "-metadata.header");

    this -> isEmptyWBuffer = true;
    ofstream outputFileStream;
    outputFileStream.open(meta_data);

    if (f_type == heap) {
        outputFileStream << "Heap file created.\n";
    }
    fBuffer -> Open(0, (char *)f_path);  
    outputFileStream.close();

    return 1;
}


void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE  *inputFileStream = fopen(loadpath , "r");
    Record currentRecord;
    while(currentRecord.SuckNextRecord( &f_schema , inputFileStream ) == 1){
        this -> Add(currentRecord);
    }
    fclose(inputFileStream);
}


int DBFile::Open (const char *f_path) {
    this -> fBuffer -> Open(1, (char *)f_path);
    this -> endOfFile = false;
    this -> pageIndex = 0;

    return 1;
}


void DBFile::MoveFirst () {
    this -> fBuffer -> GetPage(this -> rBuffer, 0);
}


int DBFile::Close () {
    if(this -> isEmptyWBuffer == false) {
        off_t numberOfPages = fBuffer -> GetLength();
        if(numberOfPages != 0) {
            numberOfPages = numberOfPages - 1;
        }
        this -> fBuffer -> AddPage(wBuffer, numberOfPages);
        wBuffer -> EmptyItOut();
    }
    this -> fBuffer -> Close();
    
    delete fBuffer;
    delete wBuffer;
    delete rBuffer;
    isEmptyWBuffer = true;

    return 1;
}

void DBFile::Add (Record &rec) {
    Record write;
    write.Consume(&rec);
    off_t numberOfPages = (*fBuffer).GetLength();

    this -> isEmptyWBuffer = false;

    if(wBuffer -> Append(&write) == 0) {
        if(numberOfPages == 0) {
            fBuffer -> AddPage(wBuffer, 0);
        }
        else {
            fBuffer -> AddPage(wBuffer , numberOfPages - 1); 
        }

        wBuffer -> EmptyItOut();
        wBuffer -> Append(&write);
    }
}


int DBFile::GetNext (Record &fetchme) {
    if(this -> endOfFile == false) {
        int result = this -> rBuffer -> GetFirst(&fetchme);
        if(result == 0) {
            pageIndex++;
            if(pageIndex >= this -> fBuffer -> GetLength() - 1) {
                this -> endOfFile = true;

                return 0; 
            } else {
                this -> fBuffer -> GetPage(this -> rBuffer,pageIndex);
                this -> rBuffer -> GetFirst(&fetchme);
            }
        }

        return 1; 
    }

    return 0; 
}


int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine compEngine;

    while(GetNext(fetchme)) {
        if(compEngine.Compare(&fetchme,&literal,&cnf))
            return 1;   
    }

    return 0; 
}


int DBFile::getSize() {
    off_t numberOfPages = this -> fBuffer -> GetLength();

    if(this -> isEmptyWBuffer == false) {
        if(numberOfPages == 0) {
            this -> fBuffer -> AddPage(this -> wBuffer, numberOfPages - 2);
        } else {
            this -> fBuffer -> AddPage(this -> wBuffer, numberOfPages - 1);
        }
        this -> isEmptyWBuffer = true;
        this -> wBuffer -> EmptyItOut();
    }

    return this -> fBuffer -> GetLength() - 1;
}