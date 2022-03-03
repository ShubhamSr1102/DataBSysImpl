#include "DBFile.h"
#include <gtest/gtest.h>

// Test to create a file.
TEST(CreateTest, ReturnValueTest){
	DBFile dbfile;

    // Check if the return value of function Create() is 1 or not.
	ASSERT_EQ(1, dbfile.Create((char *)"./gtest_dbfile", heap, NULL));
}

// Test to open a file.
TEST(OpenTest, ReturnValueTest){
	DBFile dbfile;

    // Check if the return value of function Open() is 1 or not.
	ASSERT_EQ(1, dbfile.Open((char *)"./gtest_dbfile"));
}

// Test to close a file.
TEST(CloseTest, ReturnValueTest){
	DBFile dbfile;

	dbfile.Create((char *)"./gtest_dbfile", heap, NULL); // Create a new file.

    // Check if the return value for function Close() is 1 or not.
    ASSERT_EQ(1, dbfile.Close()); 
}

// Driver code.
int main(int argc, char **argv){
	testing::InitGoogleTest(&argc, argv);

	return RUN_ALL_TESTS();
}