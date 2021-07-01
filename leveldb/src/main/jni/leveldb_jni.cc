#include "site_ycsb_db_leveldb_LevelDB.h"


void JNICALL Java_site_ycsb_db_leveldb_LevelDB_init(JNIEnv *env, jobject caller, jbyteArray folder) {
    jint length = env->GetArrayLength(folder);
    jbyte buffer[length];
    env->GetByteArrayRegion(folder, 0, length, buffer);

}

void JNICALL Java_site_ycsb_db_leveldb_LevelDB_close(JNIEnv *env, jobject caller) {

}
