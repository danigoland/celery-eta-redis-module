#define REDISMODULE_EXPERIMENTAL_API

#include "redismodule.h"
#include <stdlib.h>
#include <string.h>



int KeyExpireEventCallback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *expiringKey) {
    REDISMODULE_NOT_USED(type);
    REDISMODULE_NOT_USED(event);
    RedisModule_AutoMemory(ctx);

    size_t stringLength;
    const char *keyChar = RedisModule_StringPtrLen(expiringKey, &stringLength);

    // If doesn't contain semicolon it's of our interest since our format is <queue_name>:<task_id>
    if(strstr(keyChar, ":") == NULL){
        return REDISMODULE_OK;
    }

    char *keyString = strdup(keyChar);
    const char *queue = strtok(keyString, ":");
    char taskId[strlen(keyChar) - strlen(queue)];
    strcpy(taskId, keyChar + strlen(queue) + 1);

    // char dataKeyName[5 + strlen(queue)];
    // sprintf(dataKeyName, "data:%s", queue);
    RedisModuleString *dataKeyName = RedisModule_CreateStringPrintf(ctx, "data:%s", queue);
    RedisModuleKey *dataKey;
    dataKey = RedisModule_OpenKey(ctx, dataKeyName,
                                  REDISMODULE_READ | REDISMODULE_WRITE);

    int keyType = RedisModule_KeyType(dataKey);
    if (keyType != REDISMODULE_KEYTYPE_HASH &&
            keyType != REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_Log(ctx, "warning",
                        "Wrong Key Type: %s",
                        RedisModule_StringPtrLen(dataKeyName, &stringLength));
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    RedisModuleString *data;
    RedisModule_HashGet(dataKey, REDISMODULE_HASH_CFIELDS, taskId, &data, NULL);
    if(data == NULL){
        RedisModule_Log(ctx, "warning",
                        "%s %s is NULL)",
                        RedisModule_StringPtrLen(dataKeyName, &stringLength), taskId);
    }

    // Delete the task payload from the hashset
    RedisModule_HashSet(dataKey,REDISMODULE_HASH_CFIELDS,taskId, REDISMODULE_HASH_DELETE,NULL);

    RedisModuleKey *key;
    key = RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, queue, strlen(queue)), REDISMODULE_WRITE);
    // Push the task payload to the end of the queue(redis list)
    RedisModule_ListPush(key, REDISMODULE_LIST_TAIL, data);

    return REDISMODULE_OK;


}


/*
write readonly admin deny-oom deny-script allow-loading pubsub random allow-stale no-monitor fast getkeys-api no-cluster
*/
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "listexpiry", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    size_t len;
    /* Log the list of parameters passing loading the module. */
    for (int j = 0; j < argc; j++) {
        const char *s = RedisModule_StringPtrLen(argv[j], &len);
        printf("Module loaded with ARGV[%d] = %s\n", j, s);
    }

    RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_EXPIRED, KeyExpireEventCallback);

    return REDISMODULE_OK;
}
 