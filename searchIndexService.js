// batchSize 500,
// tableName users_working

var Q = require('q');
var elasticsearch = require('elasticsearch');
var pg = require('pg').native;

var SearchIndexService = function(options){

    var esClient = elasticsearch.Client({
      hosts: [
        options.elasticsearchEndpoint
      ]
    }); 

    var BATCH_SIZE              = options.batchSize,
        TABLE_NAME              = options.tableName,
        DB_CONNECTION_STRING    = options.dbConnectionString;

    var iterationCount = 0;

    var createIndex = function(searchOptions, fn){
        var deferred = Q.defer();

        var sql = 'SELECT * FROM ' + TABLE_NAME + ' WHERE ("user"->>\'_ninya_io_synced\')::boolean is null LIMIT ' + BATCH_SIZE;

        var rejectWithError = function(err){
            console.log('ElasticSearchSync: error ->' + err);
            deferred.reject(err);
        };

        console.log('ElasticSearchSync: connecting to postgres...');

        pg.connect(DB_CONNECTION_STRING, function(err, client, done) {
            if(err) {
                console.log('ElasticSearchSync: could not connect to postgres');
                deferred.reject(err);
                return;
            }

            console.log('ElasticSearchSync: connected to postgres');
            client.query(sql, function(err, result) {
                if(err) {
                    console.log('ElasticSearchSync: could not retrieve batch from postgres');
                    return deferred.reject(err);
                }

                console.log('ElasticSearchSync: retrieved batch from postgres');
                
                if (result.rows.length === 0){
                    console.log('ElasticSearchSync: sync finished');
                    deferred.resolve('finished');
                    return;
                }

                var tasks = result.rows.map(function(obj){

                    obj.user._ninya_location = obj.user.location.toLowerCase();

                    return esClient.index({
                        index: 'production',
                        type: 'user',
                        id: obj.user.user_id,
                        body: obj.user
                    });
                });

                Q.all(tasks)
                 .then(function(res){
                    console.log('ElasticSearchSync: added batch');
                    //mark batch as synced
                    return Q.all(result.rows.map(function(obj){
                        var user = obj.user;
                        user._ninya_io_synced = true;
                        return client.query('UPDATE ' + TABLE_NAME + ' SET "user" = $1 WHERE "user"->>\'user_id\' = \'' + user.user_id  + '\'', [user], function(err, result) {
                            if (err){
                                console.log(err);
                            }
                        });
                    }))
                    .then(function(){
                        iterationCount++;
                        done();
                        console.log('ElasticSearchSync: flagged batch as processed');
                        console.log('ElasticSearchSync: finished ' + iterationCount + ' iteration(s)');
                        //start over
                        createIndex();
                    });
                 }, rejectWithError);
            });
        });

        return deferred.promise;
    };

    return {
        createIndex: createIndex
    }
};

module.exports = SearchIndexService;