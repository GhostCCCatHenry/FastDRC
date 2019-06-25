package gene.compress;



public class BitFile {


    public static byte[] getBytes() {
        return bytes;
    }

    public static int getLen() {
        return len;
    }

    public static void setLen(int len) {
        BitFile.len = len;
    }

    private static byte[] bytes=new byte[1<<21];
    private static int len=0;
    private static int bitBuffer=0;
    private static int bitCount=0;

    public static void bitFilePutBit(int c) {

        int count=bitCount;
        int buffer=bitBuffer;

        count ++;
        buffer <<= 1;

        bitCount=count;
        bitBuffer=buffer;

        if (c != 0) {
            buffer |= 1;
            bitBuffer=buffer;
        }
        if (bitCount == 8) {
            bytes[len++]=(byte)(buffer);
            bitCount=0;
            bitBuffer=0;
        }
    }

    public static void bitFilePutChar(int c){
        int tmp;
        int count=bitCount;
        int buffer=bitBuffer;
        if (count == 0) {
            bytes[len++]=(byte)(c);
            return;
        }
        tmp = c >> count;
        tmp = tmp | getInt(buffer << (8 - count), 3);
        bytes[len++]=(byte)(tmp);
        bitBuffer=c;
    }

    public static int getInt(int num, int offset) {
        String numOfString = Integer.toBinaryString(num);
        String result;

        while (numOfString.length() < 32) {
            numOfString = "0".concat(numOfString);
        }
        char data[] = numOfString.toCharArray();
        result = new String(data, (8 * offset), 8);

        return (Integer.parseInt(result, 2));
    }

    public static void bitFilePutBitsInt(int num, int count) {    //Num is the data to be written, count is the length of the data to be written

        int offset = 3;
        int remaining = count;
        String str = "";

        while (remaining >= 8) {
            bitFilePutChar(getInt(num, offset));    //require confirmation
            remaining -= 8;
            offset --;
        }

        //Write the remaining data to the compressed file
        if (remaining != 0) {
            int tmp = getInt(num, offset);

            while(tmp != 0){
                str = tmp % 2 + str;
                tmp = tmp / 2;
            }
            while (str.length() < 8) {
                str = "0".concat(str);
            }

            char[] data = str.toCharArray();
            for (int i = 8 - remaining; i < 8; i ++) {
                bitFilePutBit(Integer.parseInt(String.valueOf(data[i])));
            }
        }
    }

    public static void clean(){

        if (bitCount != 0)
        {
            bitBuffer <<= (8 - bitCount);
            bytes[len++]=(byte) bitBuffer;

        }
    }
}
