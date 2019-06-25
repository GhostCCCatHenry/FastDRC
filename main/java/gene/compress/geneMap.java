package gene.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URLConnection;

import static gene.compress.BitFile.bitFilePutBit;
import static gene.compress.BitFile.bitFilePutBitsInt;
import static gene.compress.BitFile.bitFilePutChar;

public class geneMap extends Mapper<Text,Text,BytesWritable,NullWritable> {

    private MultipleOutputs<BytesWritable,NullWritable> bos;
    private BytesWritable byt;

    //Compress required variable


    private  int kMerLen = 12;//K-mer read length k
    private  int kMer_bit_num = 2 * kMerLen;
    private  int hashTableLen = 1 << kMer_bit_num;
    private  int MAX_CHAR_NUM = 1 << 28;
    private  int vec_size = 1 << 20;

    private  String identifier;
    private  String outPutSet ;

    private  int ref_low_vec_len = 0, tar_low_vec_len = 0, line_break_len = 0, other_char_len = 0, N_vec_len = 0, line_len = 0, ref_seq_len = 0, tar_seq_len = 0;
    private  int diff_pos_loc_len, diff_low_vec_len = 0;

    private char[] ref_seq_code;
    private  char[] tar_seq_code;
    private  int[] ref_low_vec_begin ;
    private  int[] ref_low_vec_length;
    private  int[] tar_low_vec_begin;
    private  int[] tar_low_vec_length;
    private  int[] N_vec_begin;
    private  int[] N_vec_length;
    private  int[] other_char_vec_pos ;
    private  char[] other_char_vec_ch;
    private  int[] diff_low_vec_begin ;
    private  int[] diff_low_vec_length ;
    private  int[] line_start ;
    private  int[] line_length ;
    private  int[] diff_pos_loc_begin;
    private  int[] diff_pos_loc_length;
    private  int[] line_break_vec;
    private  int[] point;
    private  int[] loc;
    private  int[] diff_low_loc ;
    private  char[] mismatched_str;
    void inicial(){
        ref_seq_code = new char[MAX_CHAR_NUM];
        tar_seq_code = new char[MAX_CHAR_NUM];
        ref_low_vec_begin = new int[vec_size];
        ref_low_vec_length = new int[vec_size];
        tar_low_vec_begin = new int[vec_size];
        tar_low_vec_length = new int[vec_size];
        N_vec_begin = new int[vec_size];
        N_vec_length = new int[vec_size];
        other_char_vec_pos = new int[vec_size];
        other_char_vec_ch = new char[vec_size];
        diff_low_vec_begin = new int[vec_size];
        diff_low_vec_length = new int[vec_size];
        line_start = new int[vec_size];
        line_length = new int[vec_size];
        diff_pos_loc_begin = new int[vec_size];
        diff_pos_loc_length = new int[vec_size];
        line_break_vec = new int[1 << 25];
        point = new int[hashTableLen];
        loc = new int[MAX_CHAR_NUM];
        diff_low_loc = new int[vec_size];
        mismatched_str = new char[vec_size];
    }

    private void SetoutPut(Context context){
        String a =((FileSplit)context.getInputSplit()).getPath().getName();
        String k1[]=a.split("_");
        String k2[]=k1[4].split("\\.");
        outPutSet=k2[3]+k2[4];
    }

    //geneMap required variable
    private boolean flag = true;
    private boolean n_flag = false;
    private int letters_len = 0;
    private int n_letters_len = 0;
    //Times
    private long startTime = System.currentTimeMillis();

    private  byte integerCoding(char ch) {
        if (ch == 'A') {
            return 0;
        }
        if (ch == 'C') {
            return 1;
        }
        if (ch == 'G') {
            return 2;
        }
        if (ch == 'T') {
            return 3;
        }
        return -1;
    }

    private  void binaryCoding(int num){
        int type;

        if (num > MAX_CHAR_NUM) {
            System.out.println("Too large to Write!\n");
            return;
        }

        if (num < 2) {
            type = 1;
            bitFilePutBitsInt(type, 2); //01
            bitFilePutBit(num);
        } else if (num < 262146) {
            type = 1;
            num -= 2;
            bitFilePutBit( type);    //1
            bitFilePutBitsInt(num, 18);
        } else {
            type = 0;
            num -= 262146;
            bitFilePutBitsInt(type, 2); //00
            bitFilePutBitsInt(num, 28);
        }
    }

    private  void searchMatchPosVec() {    //二次压缩小写字符二元组
        for (int x = 0; x < tar_low_vec_len; x ++) {
            diff_low_loc[x] = 0;
        }

        int start_position = 0, i = 0;
        out:
        while(i < tar_low_vec_len) {
            for (int j = start_position; j < ref_low_vec_len; j ++) {
                if ((tar_low_vec_begin[i] == ref_low_vec_begin[j]) && (tar_low_vec_length[i] == ref_low_vec_length[j])) {
                    diff_low_loc[i] = j;
                    start_position = j + 1;
                    i ++;
                    continue out;
                }
            }
            for (int j = start_position - 1; j > 0; j --) {
                if ((tar_low_vec_begin[i] == ref_low_vec_begin[j]) && (tar_low_vec_length[i] == ref_low_vec_length[j])) {
                    diff_low_loc[i] = j;
                    start_position = j + 1;
                    i ++;
                    continue out;
                }
            }
            diff_low_vec_begin[diff_low_vec_len] = tar_low_vec_begin[i];
            diff_low_vec_length[diff_low_vec_len ++] = tar_low_vec_length[i ++];
        }

        //diff_low_loc[i]可能是连续的数字，再次压缩成二元组
        if (tar_low_vec_len > 0) {
            int cnt = 1;
            diff_pos_loc_begin[diff_pos_loc_len] = diff_low_loc[0];
            for (int x = 1; x < tar_low_vec_len; x ++) {
                if ((diff_low_loc[x] - diff_low_loc[x - 1]) == 1) {
                    cnt ++;
                } else {
                    diff_pos_loc_length[diff_pos_loc_len ++] = cnt;
                    diff_pos_loc_begin[diff_pos_loc_len] = diff_low_loc[x];
                    cnt = 1;
                }
            }
            diff_pos_loc_length[diff_pos_loc_len ++] = cnt;
        }

        if (line_break_len > 0) {
            int cnt = 1;
            line_start[line_len] = line_break_vec[0];
            for (int j = 1; j < line_break_len; j ++) {
                if (line_start[line_len] == line_break_vec[j]) {
                    cnt ++;
                } else {
                    line_length[line_len ++] = cnt;
                    line_start[line_len] = line_break_vec[j];
                    cnt = 1;
                }
            }
            line_length[line_len ++] = cnt;
        }

    }

    private  void saveOtherData() {

        binaryCoding(identifier.length());
        char []meta_data=identifier.toCharArray();
        for(int i = 0; i < identifier.length(); i ++) {
            bitFilePutChar(meta_data[i]);
        }
        binaryCoding(line_len);
        for (int i = 0; i < line_len; i ++) {
            binaryCoding(line_start[i]);
            binaryCoding(line_length[i]);
        }

        binaryCoding(diff_pos_loc_len);
        for (int i = 0; i < diff_pos_loc_len; i ++) {
            binaryCoding(diff_pos_loc_begin[i]);
            binaryCoding(diff_pos_loc_length[i]);
        }

        binaryCoding(diff_low_vec_len);
        for (int i = 0; i < diff_low_vec_len; i ++) {
            binaryCoding(diff_low_vec_begin[i]);
            binaryCoding(diff_low_vec_length[i]);
        }

        binaryCoding(N_vec_len);
        for (int i = 0; i < N_vec_len; i ++) {
            binaryCoding(N_vec_begin[i]);
            binaryCoding(N_vec_length[i]);
        }

        binaryCoding(other_char_len);
        if (other_char_len > 0) {
            for(int i = 0; i < other_char_len; i ++){
                binaryCoding(other_char_vec_pos[i]);
                bitFilePutChar(other_char_vec_ch[i] - 'A');
            }
        }
    }

    private  void kMerHashingConstruct() {
        int value = 0;
        int step_len = ref_seq_len - kMerLen + 1;
        for (int i = 0; i < hashTableLen; i ++) {
            point[i] = -1;
        }

        for (int k = kMerLen - 1; k >= 0; k --) {
            value <<= 2;
            value += integerCoding(ref_seq_code[k]);
        }
        loc[0] = point[value];
        point[value] = 0;

        int shift_bit_num = (kMerLen * 2 - 2);
        int one_sub_str = kMerLen - 1;
        for (int i = 1; i < step_len; i ++) {
            value >>= 2;
            value += (integerCoding(ref_seq_code[i + one_sub_str])) << shift_bit_num;
            loc[i] = point[value];
            point[value] = i;
        }
    }
    private  void searchMatchSeqCode(){

        int pre_pos = 0, misLen_total = 0;
        int step_len = tar_seq_len - kMerLen + 1;   //step_len = 34170096,有step_len组kMer
        int max_length, max_k;

        //The hash threshold of matched sequence m
        int min_rep_len = 35;
        int misLen = 0, i, j, k, id, ref_idx, tar_idx, length, cur_pos;
        int tar_value;

        for (i = 0; i < step_len; i ++) {
            tar_value = 0;
            for (j = kMerLen - 1; j >= 0; j --) {
                tar_value <<= 2;
                tar_value += integerCoding(tar_seq_code[i + j]);
            }

            id = point[tar_value];
            if (id > -1) {  //match successful
                max_length = -1;
                max_k = -1;

                for (k = id; k != -1; k = loc[k]) {
                    ref_idx = k + kMerLen;
                    tar_idx = i + kMerLen;
                    length = kMerLen;
                    while (ref_idx < ref_seq_len && tar_idx < tar_seq_len && ref_seq_code[ref_idx ++] == tar_seq_code[tar_idx ++]) {
                        length ++;
                    }
                    if (length >= min_rep_len && length > max_length) {
                        max_length = length;
                        max_k = k;
                    }
                }

                if (max_k > -1) {
                    //After finding the maximum match, first write the unmatched file between the tar, including the length and characters.
                    binaryCoding(misLen);
                    if (misLen > 0) {
                        misLen_total += misLen;
                        //2-bit coding for mismatched string
                        for(int m = 0; m < misLen; m ++){
                            int num = integerCoding(mismatched_str[m]);
                            bitFilePutBitsInt(num, 2);
                        }
                        misLen = 0;
                    }
                    cur_pos = max_k - pre_pos;
                    pre_pos = max_k + max_length;
                    if (cur_pos < 0) {
                        cur_pos = -cur_pos;
                        bitFilePutBit(1);

                    } else {
                        bitFilePutBit(0);
                    }
                    binaryCoding(cur_pos);
                    binaryCoding(max_length - min_rep_len);
                    i += (max_length - 1);//The next loop directly jumps to the back of the matching segment to calculate the hash value.
                    continue;
                }
            }
            mismatched_str[misLen ++] = tar_seq_code[i];
        }
        for(; i < tar_seq_len; i++) {
            mismatched_str[misLen ++] = tar_seq_code[i];
        }
        binaryCoding(misLen);
        if (misLen > 0) {
            for(int x = 0; x < misLen; x ++) {
                int num=integerCoding(mismatched_str[i]);
                bitFilePutBitsInt(num,2);
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        bos=new MultipleOutputs<>(context);
        byt=new BytesWritable();

        inicial();
        super.setup(context);

        SetoutPut(context);
        Configuration conf = context.getConfiguration();
        //分布式缓存读文件
        URI[] cacheFile = context.getCacheFiles();
        Path file_path = new Path(cacheFile[0].toString());
        FSDataInputStream inputCache = FileSystem.get(cacheFile[0], conf).open(file_path);
        BufferedReader br = new BufferedReader(new InputStreamReader(inputCache));

        String str;
        int str_length;
        char ch;
        Boolean flag = true;
        int letters_len = 0;
        br.readLine();
        while ((str = br.readLine()) != null) {
            str_length = str.length();
            for (int i = 0; i < str_length; i++) {
                ch = str.charAt(i);

                if (Character.isLowerCase(ch)) {
                    ch = Character.toUpperCase(ch);

                    if (flag) {
                        flag = false;
                        ref_low_vec_begin[ref_low_vec_len] = letters_len;
                        letters_len = 0;
                    }
                } else {
                    if (!flag) {
                        flag = true;
                        ref_low_vec_length[ref_low_vec_len++] = letters_len;
                        letters_len = 0;
                    }
                }

                if (ch == 'A' || ch == 'C' || ch == 'G' || ch == 'T') {
                    ref_seq_code[ref_seq_len++] = ch;
                }

                letters_len++;
            }
        }
        if (!flag) {
            ref_low_vec_length[ref_low_vec_len++] = letters_len;
        }

        br.close();
        inputCache.close();
        //hash编码
        kMerHashingConstruct();

    }

    /**
     * Iterative map function, try not to create new objects in it, be sure to pay attention to the division of key/value!
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String line; char ch; int str_length; char []tarFile;

        line=key.toString();
        if(line.contains("gb")||line.contains("chr")){
            identifier=line;
        }else {
            tarFile=line.toCharArray();
            str_length = tarFile.length;
            for (int i = 0; i < str_length; i++) {
                ch = tarFile[i];

                if (Character.isLowerCase(ch)) {
                    ch = Character.toUpperCase(ch);

                    if (flag) {
                        flag = false;
                        tar_low_vec_begin[tar_low_vec_len] = letters_len;
                        letters_len = 0;
                    }
                } else {
                    if (!flag) {
                        flag = true;
                        tar_low_vec_length[tar_low_vec_len ++] = letters_len;
                        letters_len = 0;
                    }
                }
                letters_len ++;

                if(ch == 'A'||ch == 'G'||ch == 'C'||ch == 'T') {
                    tar_seq_code[tar_seq_len ++] = ch;
                } else if(ch != 'N') {
                    other_char_vec_pos[other_char_len] = tar_seq_len;
                    other_char_vec_ch[other_char_len ++] = ch;
                }

                if (!n_flag) {
                    if (ch == 'N') {
                        N_vec_begin[N_vec_len] = n_letters_len;
                        n_letters_len = 0;
                        n_flag = true;
                    }
                } else {
                    if (ch != 'N'){
                        N_vec_length[N_vec_len ++] = n_letters_len;
                        n_letters_len = 0;
                        n_flag = false;
                    }
                }
                n_letters_len++;
            }
            line_break_vec[line_break_len ++] = str_length;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //这些只能执行一次，在map迭代完成之后执行
        if (!flag) {
            tar_low_vec_length[tar_low_vec_len++] = letters_len;
        }

        if (n_flag) {
            N_vec_length[N_vec_len++] = n_letters_len;
        }

        for (int i = other_char_len - 1; i > 0; i --) {
            other_char_vec_pos[i] -= other_char_vec_pos[i - 1];
        }

        //设置bit输出类中的MultipleOutputs
        //MyRecordWriter my = (MyRecordWriter) new myMultipleOutput().getRecordWriter(context);

        //BitFile.setWriter(my);

        searchMatchPosVec();

        saveOtherData();

        searchMatchSeqCode();

        //BitFile.setContext(context);

        BitFile.clean();
        byt.set(BitFile.getBytes(),0,BitFile.getLen());
        bos.write(outPutSet,byt,NullWritable.get());

        bos.close();
        BitFile.setLen(0);

        System.out.println("total耗费时间为" + (System.currentTimeMillis() - startTime) + "ms");
    }


}


