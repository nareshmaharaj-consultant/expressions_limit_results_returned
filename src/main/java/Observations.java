import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.*;
import com.aerospike.client.exp.*;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import org.junit.Test;
import java.io.IOException;
import java.util.*;

public class Observations {

    private static final int LIST_INDEX_POS_FOR_OBS_CUTOFF = 30;
    private static final String LIST_INDEX_BIN_NAME_FOR_OBS_CUTOFF = "indexOfReqObs";

    String namespace = "test";
    String set = "neustar";
    String primaryKey = "1";

    String host = "localhost";
    int port = 3000;

    String observationBinName = "observations";
    String listOfObsBinName = "obs";
    String sumObsBinName = "totalObs";

    int ITEMS_MAPKEYS = 100;
    int ITEMS_DATAPOINTS = 750;

    public Observations() throws Exception {}

    private AerospikeClient getClient(){
        Host[] hosts = new Host[] { new Host( host, port) };
        ClientPolicy clientPolicy = new ClientPolicy();
        return new AerospikeClient(clientPolicy, hosts);
    }

    @Test
    public void add() throws Exception {

        Key key = new Key(namespace, set, primaryKey);

        // First set of observations
        List<Value> obsValues = new ArrayList<>();
        obsValues.add(Value.get(1));
        obsValues.add(Value.get(2));
        obsValues.add(Value.get(3));
        addObservations(key, obsValues);

        // Second set of observations
        obsValues = new ArrayList<>();
        obsValues.add(Value.get(4));
        obsValues.add(Value.get(5));
        obsValues.add(Value.get(6));
        obsValues.add(Value.get(7));
        addObservations(key, obsValues);

        // Third set of observartions
        obsValues = new ArrayList<>();
        obsValues.add(Value.get(13));
        obsValues.add(Value.get(14));
        addObservations(key, obsValues);
    }

    @Test
    public void readIncludingRange() throws Exception {
        AerospikeClient client = getClient();
        Key key = new Key(namespace, set, primaryKey);

        Expression exp3 = Exp.build(
                Exp.cond(
                        Exp.binExists( LIST_INDEX_BIN_NAME_FOR_OBS_CUTOFF ),
                        ListExp.getByIndexRange(ListReturnType.VALUE, Exp.val(0), Exp.intBin(LIST_INDEX_BIN_NAME_FOR_OBS_CUTOFF), Exp.listBin(observationBinName)),
                        ListExp.getByIndexRange(ListReturnType.VALUE, Exp.val(0), Exp.val(-1), Exp.listBin(observationBinName))
                )
        );

        Record r3 = client.operate( null, key,
                ExpOperation.read("res3", exp3, ExpReadFlags.DEFAULT)
        );
        System.out.println(r3);
    }


    private boolean addObservations( Key key,  List<Value> obsValues ) {

        AerospikeClient client = getClient();
        BatchPolicy batchPolicy = new BatchPolicy(client.writePolicyDefault);
        batchPolicy.setTimeout(1000);

        List<BatchRecord> batchRecords = new ArrayList<>();

        Map<Value, Value> mapObservations = new HashMap<>();
        mapObservations.put( Value.get(listOfObsBinName), Value.get(obsValues));


        Expression cumulativeObsCount = Exp.build(
                Exp.add(
                        Exp.cond(
                                Exp.binExists( sumObsBinName ), Exp.intBin( sumObsBinName ), Exp.val(0)
                        ),
                        Exp.val( obsValues.size() )
                )
        );

        Expression listIndexOfRequiredObs = Exp.build(
                Exp.cond(
                        Exp.not( Exp.binExists( LIST_INDEX_BIN_NAME_FOR_OBS_CUTOFF )),
                        Exp.cond(
                                Exp.ge( Exp.intBin( sumObsBinName ), Exp.val(LIST_INDEX_POS_FOR_OBS_CUTOFF) ),
                                ListExp.size(Exp.listBin(observationBinName)),
                                Exp.unknown()
                        ),
                        Exp.unknown()
                )
        );

        Operation[] operations = Operation.array(
                ListOperation.append(observationBinName, Value.get(mapObservations)),
                ExpOperation.write(sumObsBinName, cumulativeObsCount, ExpWriteFlags.DEFAULT),
                ExpOperation.write(LIST_INDEX_BIN_NAME_FOR_OBS_CUTOFF, listIndexOfRequiredObs, ExpWriteFlags.EVAL_NO_FAIL)

        );
        batchRecords.add(new BatchWrite(key,operations));
        return client.operate(batchPolicy,batchRecords);
    }



    /**
     * The following is simply testing if we can reduce the size of the record.
     * Use Maps as part of the schema
     *
     * {
     *   "data": [
     *     [
     *       {
     *         "obs": [
     *           {
     *             "d": 9223372036854775807
     *           },
     *           {
     *             "d": 9223372036854775807
     *           }
     *         ]
     *       },
     *       {
     *         "obs": [
     *           {
     *             "d": 9223372036854775807
     *           },
     *           {
     *             "d": 9223372036854775807
     *           }
     *         ]
     *       }
     *     ]
     *   ]
     * }
     * Data size is 953.094 KB
     *
     *
     */
    @Test
    public void createListWithDocumentValues()
    {
        Key key = new Key(namespace, set, primaryKey);

        AerospikeClient client = getClient();
        client.delete(null,key);
        BatchPolicy batchPolicy = new BatchPolicy(client.writePolicyDefault);
        batchPolicy.setTimeout(50000);

        List<BatchRecord> batchRecords = new ArrayList<>();

        List<Value> data = new ArrayList<>();                   // Outer List

        for ( int i=0; i< ITEMS_MAPKEYS; i++){
            List<Value> obs = new ArrayList<>();
            Map<Value, Value> mapKey = new HashMap<>();         // Create 100 MapKeys
            mapKey.put( Value.get("obs"), Value.get(obs) );
            for ( int j=0; j < ITEMS_DATAPOINTS; j++ ) {
                Map<Value, Value> datapoint = new HashMap<>();  // Create 1000 datapoint Maps
                datapoint.put( Value.get("d"), Value.get(Long.MAX_VALUE) );
                obs.add(Value.get(datapoint));                  // Add to Obs List
            }
            data.add(Value.get(mapKey));
        }
        Operation[] operations = Operation.array(
                ListOperation.append("data", Value.get(data))
        );
        batchRecords.add(new BatchWrite(key,operations));
        boolean success = client.operate(batchPolicy,batchRecords);
        System.out.println( success );
    }


    /**
     * Removes the inner map inside obs
     *
     * {
     *   "data": [
     *     [
     *       {
     *         "obs": [
     *           9223372036854775807,
     *           9223372036854775807
     *         ]
     *       },
     *       {
     *         "obs": [
     *           9223372036854775807,
     *           9223372036854775807
     *         ]
     *       }
     *     ]
     *   ]
     * }
     *
     * Size: 660.125 KB
     */
    @Test
    public void createListWithLongValuesNoInnerMap()
    {
        Key key = new Key(namespace, set, primaryKey);

        AerospikeClient client = getClient();
        BatchPolicy batchPolicy = new BatchPolicy(client.writePolicyDefault);
        batchPolicy.setTimeout(50000);
        client.delete(null,key);

        List<BatchRecord> batchRecords = new ArrayList<>();

        List<Value> data = new ArrayList<>();                   // Outer List

        for ( int i=0; i< ITEMS_MAPKEYS; i++){
            List<Value> obs = new ArrayList<>();
            Map<Value, Value> mapKey = new HashMap<>();         // Create 100 MapKeys
            mapKey.put( Value.get("obs"), Value.get(obs) );
            for ( int j=0; j < ITEMS_DATAPOINTS; j++ ) {
                obs.add(Value.get(Long.MAX_VALUE));                  // Add to Obs List
            }

            data.add(Value.get(mapKey));
        }
        Operation[] operations = Operation.array(
                ListOperation.append("data", Value.get(data))
        );
        batchRecords.add(new BatchWrite(key,operations));
        boolean success = client.operate(batchPolicy,batchRecords);
        System.out.println( success );
    }


    /**
     * Does not use any maps whatsoever anywhere
     *
     * {
     *   "data": [
     *     [
     *       [
     *         9223372036854775807,
     *         9223372036854775807
     *       ],
     *       [
     *         9223372036854775807,
     *         9223372036854775807
     *       ]
     *     ]
     *   ]
     * }
     *
     * Size: 659.547 KB
     * @throws IOException
     */
    @Test
    public void createListWithLongValuesNoMaps() throws IOException {
        Key key = new Key(namespace, set, primaryKey);

        AerospikeClient client = getClient();
        client.delete(null,key);
        BatchPolicy batchPolicy = new BatchPolicy(client.writePolicyDefault);
        batchPolicy.setTimeout(50000);

        List<BatchRecord> batchRecords = new ArrayList<>();

        List<Value> data = new ArrayList<>();                   // Outer List

        for ( int i=0; i< ITEMS_MAPKEYS; i++){
            List<Value> obs = new ArrayList<>();
            for ( int j=0; j < ITEMS_DATAPOINTS; j++ ) {
                obs.add(Value.get(Long.MAX_VALUE));                  // Add to Obs List
            }

            data.add(Value.get(obs));
        }
        Operation[] operations = Operation.array(
                ListOperation.append("data", Value.get(data))
        );
        batchRecords.add(new BatchWrite(key,operations));
        boolean success = client.operate(batchPolicy,batchRecords);
        System.out.println( success );
    }

    /**
     * Compressed:
     * manage config namespace test param compression to zstd
     * manage config namespace test param enable-compression;compression-level to 1
     *
     * Down to
     *  208.000 B
     *  176.000 B
     *  176.000 B
     *
     *  Respectively
     */
}
