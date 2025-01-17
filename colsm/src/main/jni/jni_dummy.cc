/**
 *  This is a dummy implementation of LevelDB JNI for easy test of JNI functionality
 */
#include <map>
#include <fstream>
#include "site_ycsb_db_colsm_CoLSM.h"

std::string fromByteArray(JNIEnv *env, jbyteArray input) {
    jint length = env->GetArrayLength(input);
    std::string result;
    result.resize(length);
    env->GetByteArrayRegion(input, 0, length, (jbyte *) result.data());
    return result;
}

//inline jint translate(leveldb::Status status) { return status.intcode(); }

static jclass levelDB_Class;
static jfieldID levelDB_db;

jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
    // Obtain the JNIEnv from the VM and confirm JNI_VERSION
    JNIEnv *env;
    if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_9) != JNI_OK) {
        return JNI_ERR;
    }

    jclass tempLocalClassRef = env->FindClass("site/ycsb/db/colsm/CoLSM");
    levelDB_Class = (jclass) env->NewGlobalRef(tempLocalClassRef);
    env->DeleteLocalRef(tempLocalClassRef);

    // Load the method id
    levelDB_db = env->GetFieldID(levelDB_Class, "db", "J");

    return JNI_VERSION_9;
}

void JNICALL JNI_OnUnload(JavaVM *vm, void *reserved) {
    // Obtain the JNIEnv from the VM
    // NOTE: some re-do the JNI Version check here, but I find that redundant
    JNIEnv *env;
    vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_9);
    // Destroy the global references
    env->DeleteGlobalRef(levelDB_Class);
}

void JNICALL Java_site_ycsb_db_colsm_CoLSM_init(JNIEnv *env, jobject caller,
                                                    jbyteArray folder) {
    std::string folder_name = fromByteArray(env, folder);
    std::map<int32_t, std::string> *storage = new std::map<int32_t, std::string>();
    env->SetLongField(caller, levelDB_db, (int64_t) storage);
}

void JNICALL Java_site_ycsb_db_colsm_CoLSM_close(JNIEnv *env,
                                                     jobject caller) {
    std::map<int32_t, std::string> *storage = (std::map<int32_t, std::string> *) env->GetLongField(caller, levelDB_db);
    delete storage;
}

using namespace std;
jint JNICALL Java_site_ycsb_db_colsm_CoLSM_put(JNIEnv *env, jobject caller,
                                                   jbyteArray jkey,
                                                   jbyteArray jvalue) {
    std::map<int32_t, std::string> *storage = (std::map<int32_t, std::string> *) env->GetLongField(caller, levelDB_db);
    auto key = fromByteArray(env, jkey);
    int32_t intkey = *((int32_t *) key.data());
//    auto value = fromByteArray(env, jvalue);
//    (*storage)[intkey] = value;
//    return 0;
    ofstream fs;
    fs.open("output");
    fs << "Put " << intkey << '\n';
    fs.close();
    return intkey;
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_colsm_CoLSM_delete(
        JNIEnv *env, jobject caller, jbyteArray jkey) {
    std::map<int32_t, std::string> *storage = (std::map<int32_t, std::string> *) env->GetLongField(caller, levelDB_db);
    auto key = fromByteArray(env, jkey);
    int32_t intkey = *((int32_t *) key.data());
//    storage->erase(intkey);
//    return 0;
    return intkey;
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_colsm_CoLSM_get(
        JNIEnv *env, jobject caller, jbyteArray jkey, jobjectArray jvalue) {
    std::map<int32_t, std::string> *storage = (std::map<int32_t, std::string> *) env->GetLongField(caller, levelDB_db);
    auto key = fromByteArray(env, jkey);
    int32_t intkey = *((int32_t *) key.data());
//    auto found = storage->find(intkey);
//    if (found == storage->end()) {
//        return 1;
//    }
//    auto value = found->second;
//    auto bytearray = env->NewByteArray(value.size());
//    env->SetByteArrayRegion(bytearray, 0, value.size(), (const jbyte *) value.data());
//    env->SetObjectArrayElement(jvalue, 0, bytearray);
//    return 0;
    return intkey;
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_colsm_CoLSM_scan(
        JNIEnv *env, jobject caller, jbyteArray jkey, jint limit,
        jobjectArray jvalues) {
    std::map<int32_t, std::string> *storage = (std::map<int32_t, std::string> *) env->GetLongField(caller, levelDB_db);
    auto key = fromByteArray(env, jkey);
    int32_t intkey = *((int32_t *) key.data());
    return intkey;
//    auto ite = storage->lower_bound(intkey);
//    for (int i = 0; i < limit; ++i) {
//        if (ite == storage->end()) {
//            return 0;
//        }
//        auto value = ite->second;
//        auto bytearray = env->NewByteArray(value.size());
//        env->SetByteArrayRegion(bytearray, 0, value.size(), (const jbyte *) value.data());
//        env->SetObjectArrayElement(jvalues, i, bytearray);
//        ite++;
//    }
//    return 0;
}
