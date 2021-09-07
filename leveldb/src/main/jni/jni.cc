#include <leveldb/db.h>
#include <leveldb/filter_policy.h>
#include <leveldb/comparator.h>

#include "site_ycsb_db_leveldb_LevelDB.h"

std::string fromByteArray(JNIEnv* env, jbyteArray input) {
  jint length = env->GetArrayLength(input);
  std::string result;
  result.resize(length);
  env->GetByteArrayRegion(input, 0, length, (jbyte*)result.data());
  return result;
}

inline jint translate(leveldb::Status status) {
  if (status.ok()) {
    return 0;
  }
  if (status.IsNotFound()) {
    return 1;
  }
  if (status.IsCorruption()) {
    return 2;
  }
  if (status.IsNotSupportedError()) {
    return 3;
  }
  if (status.IsInvalidArgument()) {
    return 4;
  }
  return 5;
}

static jclass levelDB_Class;
static jfieldID levelDB_db;
// Filter Policy
static jfieldID levelDB_fp;

jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved) {
  // Obtain the JNIEnv from the VM and confirm JNI_VERSION
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_9) != JNI_OK) {
    return JNI_ERR;
  }

  jclass tempLocalClassRef = env->FindClass("site/ycsb/db/leveldb/LevelDB");
  levelDB_Class = (jclass)env->NewGlobalRef(tempLocalClassRef);
  env->DeleteLocalRef(tempLocalClassRef);

  // Load the method id
  levelDB_db = env->GetFieldID(levelDB_Class, "db", "J");
  levelDB_fp = env->GetFieldID(levelDB_Class, "fp", "J");

  return JNI_VERSION_9;
}

void JNICALL JNI_OnUnload(JavaVM* vm, void* reserved) {
  // Obtain the JNIEnv from the VM
  // NOTE: some re-do the JNI Version check here, but I find that redundant
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_9);
  // Destroy the global references
  env->DeleteGlobalRef(levelDB_Class);
}

void JNICALL Java_site_ycsb_db_leveldb_LevelDB_init(JNIEnv* env, jobject caller,
                                                    jbyteArray folder) {
  std::string folder_name = fromByteArray(env, folder);

  leveldb::DB* db;

  leveldb::Options options;
  options.comparator = leveldb::BytewiseComparator();
  options.create_if_missing = true;
  options.compression = leveldb::kNoCompression;
  options.block_size = 2*1024*1024;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status status = leveldb::DB::Open(options, folder_name, &db);
  if (status.ok()) {
    env->SetLongField(caller, levelDB_db, (int64_t)db);
    env->SetLongField(caller, levelDB_fp, (int64_t)options.filter_policy);
  }
}

void JNICALL Java_site_ycsb_db_leveldb_LevelDB_close(JNIEnv* env,
                                                     jobject caller) {
  leveldb::DB* db = (leveldb::DB*)env->GetLongField(caller, levelDB_db);
  leveldb::FilterPolicy* fp = (leveldb::FilterPolicy*)env->GetLongField(caller, levelDB_fp);
  delete db;
  delete fp;
}

jint JNICALL Java_site_ycsb_db_leveldb_LevelDB_put(JNIEnv* env, jobject caller,
                                                   jbyteArray jkey,
                                                   jbyteArray jvalue) {
  leveldb::DB* db = (leveldb::DB*)env->GetLongField(caller, levelDB_db);
  auto key = fromByteArray(env, jkey);
  auto value = fromByteArray(env, jvalue);
  auto status = db->Put(leveldb::WriteOptions(), leveldb::Slice(key),
                        leveldb::Slice(value));
  return translate(status);
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_leveldb_LevelDB_delete(
    JNIEnv* env, jobject caller, jbyteArray jkey) {
  leveldb::DB* db = (leveldb::DB*)env->GetLongField(caller, levelDB_db);
  auto key = fromByteArray(env, jkey);
  auto status = db->Delete(leveldb::WriteOptions(), leveldb::Slice(key));
  return translate(status);
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_leveldb_LevelDB_get(
    JNIEnv* env, jobject caller, jbyteArray jkey, jobjectArray jvalue) {
  leveldb::DB* db = (leveldb::DB*)env->GetLongField(caller, levelDB_db);
  auto key = fromByteArray(env, jkey);
  std::string value;
  auto status = db->Get(leveldb::ReadOptions(), key, &value);
  if (status.ok()) {
    jbyteArray jresult = env->NewByteArray(value.size());
    env->SetByteArrayRegion(jresult, 0, value.size(),
                            (const jbyte*)value.data());
    env->SetObjectArrayElement(jvalue, 0, jresult);
  }
  return translate(status);
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_leveldb_LevelDB_scan(
    JNIEnv* env, jobject caller, jbyteArray jkey, jint limit,
    jobjectArray jvalues) {
  leveldb::DB* db = (leveldb::DB*)env->GetLongField(caller, levelDB_db);
  auto key = fromByteArray(env, jkey);
  auto iterator = db->NewIterator(leveldb::ReadOptions());
  iterator->Seek(leveldb::Slice(key));
  for (auto i = 0; i < limit; ++i) {
    if (iterator->Valid()) {
      auto value = iterator->value();
      auto jvalue = env->NewByteArray(value.size());
      env->SetByteArrayRegion(jvalue, 0, value.size(),
                              (const jbyte*)value.data());
      env->SetObjectArrayElement(jvalues, i, jvalue);
      iterator->Next();
    }
  }
  delete iterator;
  return 0;
}
