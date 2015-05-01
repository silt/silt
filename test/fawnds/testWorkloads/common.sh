
# just to check the format/validity of the source YCSB data
function generate_load_raw {
    # $1 = filename prefix for a "load" trace
    # $2 = (UNUSED)
    # $3 = arguments to YCSB

    echo generating $1.load.raw

    #java -cp ${YCSB_DIR}/build/ycsb.jar com.yahoo.ycsb.Client -load -db com.yahoo.ycsb.BasicDB $3 -s > $1.load.raw
    java -cp ${YCSB_DIR}/core/lib/core-0.1.4.jar com.yahoo.ycsb.Client -load -db com.yahoo.ycsb.BasicDB $3 -s > $1.load.raw
}

function generate_load {
    # $1 = filename prefix for a "load" trace
    # $2 = arguments to preprocessTrace
    # $3 = arguments to YCSB

    echo generating $1.load

    #java -cp ${YCSB_DIR}/build/ycsb.jar com.yahoo.ycsb.Client -load -db com.yahoo.ycsb.BasicDB $3 -s | ../preprocessTrace $2 $1.load
    java -cp ${YCSB_DIR}/core/lib/core-0.1.4.jar com.yahoo.ycsb.Client -load -db com.yahoo.ycsb.BasicDB $3 -s | ../preprocessTrace $2 $1.load
}

function generate_trans {
    # $1 = filename prefix for a "transaction" trace
    # $2 = arguments to preprocessTrace
    # $3 = arguments to YCSB

    echo generating $1.trans

    #java -cp ${YCSB_DIR}/build/ycsb.jar com.yahoo.ycsb.Client -t -db com.yahoo.ycsb.BasicDB $3 -s | ../preprocessTrace $2 $1.trans
    java -cp ${YCSB_DIR}/core/lib/core-0.1.4.jar com.yahoo.ycsb.Client -t -db com.yahoo.ycsb.BasicDB $3 -s | ../preprocessTrace $2 $1.trans
}

if [ $# -ne 1 ] 
then 
    echo "Usage: $0 [path-to-YSCB]"
    echo "	e.g. $0 /Users/binfan/projects/fawn/YCSB"
    exit 1
fi

YCSB_DIR=$1

