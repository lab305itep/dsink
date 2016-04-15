#!/bin/bash

DATA_DIR=data
MIN_SPACE=30720
MOUNT_BASE=/data

# Usage: get_word num word1 word2 ...
# Return the word number num
get_word ()
{
    local num=$1
    shift $num
    echo $1
}

# Usage: get_free dir
# Return free space (in Mb) on the device where directory dir is located
get_free ()
{
    local tmp=`df -BM $1 2> /dev/null | grep /sd`
    tmp=`get_word 4 $tmp`
    if [ "$tmp"X == "X" ] ; then 
	echo "0"
	return
    fi
    echo ${tmp/M/}
}

# Usage: get_data_num dir
# Return number of the disk from its label.
# Disk is pointed by directory dir located on it.
# Label must be of form DATANN. NN is the number (1 to 4 digits).
# For wrong label format -1 is returned.
get_data_num ()
{
    local tmp=`df $1 2> /dev/null | grep /dev/`
    tmp=`get_word 1 $tmp`
    if [ "$tmp"X == "X" ] ; then 
	echo "-1"
	return
    fi
    tmp=`lsblk -f $tmp | grep DATA`
    tmp=`get_word 3 $tmp`
    if [ "${tmp:0:4}" != "DATA" ] ; then
	echo "-1"
	return
    fi
    tmp=${tmp:4}
    if [ "$tmp"X == "X" ] ; then 
	echo "-1"
	return
    fi
    local emp=${tmp#[0-9]}
    emp=${emp#[0-9]}
    emp=${emp#[0-9]}
    emp=${emp#[0-9]}
    if [ "$emp"X != "X" ] ; then 
	echo "-1"
	return
    fi
    echo $tmp
}


if [ $(get_free $DATA_DIR) -ge $MIN_SPACE ] ; then
	exit 0
fi

declare -i NUM

if [ -h $DATA_DIR ] ; then
    NUM=`get_data_num $DATA_DIR`
    unlink $DATA_DIR
else
    NUM=-1
fi

declare -a LIST

LIST=(`ls $MOUNT_BASE | sort -n`)
for ((n=0; $n<${#LIST[@]}; n=$n+1)); do
	[ "$(mount | grep $MOUNT_BASE"/"${LIST[n]})"X == "X" ] && continue
	TARGET="$MOUNT_BASE"/"${LIST[n]}"/data
	[ ${LIST[n]} -le $NUM ] && continue
	[ $(get_free $TARGET) -lt $MIN_SPACE ] && continue
	[ ! -w $TARGET ] && continue
	ln -s $TARGET $DATA_DIR
	exit 0
done

exit 1
