package com.asiainfo.ocdc.streaming.source.socket;

/**
 * @author surq<br>
 * @since 2015.5.11<br>
 * 接收socket数据工具类<br>
 * @param msg
 */
public class socketUtil {

	public static void main(String[] args) {

	}


    public static int toInt(byte[] bArr) {
        int iout = 0;
        byte bLoop;
        for (int i = 0; i < 2; i++) {
            bLoop = bArr[i];
            iout += (bLoop & 0xFF) << (8 * i);
        }
        return iout;
    }

    public static String negativeIntToHex(int i) {
        // 负整数时，前面输入了多余的 FF ，没有去掉前面多余的 FF，按并双字节形式输出
        return Integer.toHexString(i);// FFFFFFFE
    }

    public static int HexToNegativeInt(String hex) {
        return Integer.valueOf(hex, 16);
    }

	/**
	 * byte[] to int<br>
	 * @param bytes
	 * @return
	 */
    public static int bytesToInt(byte[] bytes) {
        int num = bytes[1] & 0xFF;
        num |= ((bytes[0] << 8) & 0xFF00);
        return num;
    }
    

	/**
	 * socket请求信息<br>
	 * 
	 * @return
	 */
	public static byte[] getMsgConnect() {
		byte[] msgConnect = new byte[47];
		msgConnect[0] = (byte) 0x9e;
		msgConnect[1] = (byte) 0x62;
		msgConnect[2] = (byte) 0x00;
		msgConnect[3] = (byte) 0x2a;
		msgConnect[4] = (byte) 0x00;
		msgConnect[5] = (byte) 0x00;
		msgConnect[6] = (byte) 0x01;
		msgConnect[7] = (byte) 0x00;
		msgConnect[8] = (byte) 0x00;
		msgConnect[9] = (byte) 0x00;
		msgConnect[10] = (byte) 0x00;
		msgConnect[11] = (byte) 0x7a;
		msgConnect[12] = (byte) 0x78;
		msgConnect[13] = (byte) 0x74;
		msgConnect[14] = (byte) 0x32;
		msgConnect[15] = (byte) 0x30;
		msgConnect[16] = (byte) 0x30;
		msgConnect[17] = (byte) 0x30;
		msgConnect[18] = (byte) 0x00;
		msgConnect[19] = (byte) 0x00;
		msgConnect[20] = (byte) 0x00;
		msgConnect[21] = (byte) 0x00;
		msgConnect[22] = (byte) 0x00;
		msgConnect[23] = (byte) 0x00;
		msgConnect[24] = (byte) 0x00;
		msgConnect[25] = (byte) 0x00;
		msgConnect[26] = (byte) 0x00;
		msgConnect[27] = (byte) 0x7a;
		msgConnect[28] = (byte) 0x78;
		msgConnect[29] = (byte) 0x74;
		msgConnect[30] = (byte) 0x32;
		msgConnect[31] = (byte) 0x30;
		msgConnect[32] = (byte) 0x30;
		msgConnect[33] = (byte) 0x30;
		msgConnect[34] = (byte) 0x00;
		msgConnect[35] = (byte) 0x00;
		msgConnect[36] = (byte) 0x00;
		msgConnect[37] = (byte) 0x00;
		msgConnect[38] = (byte) 0x00;
		msgConnect[39] = (byte) 0x00;
		msgConnect[40] = (byte) 0x00;
		msgConnect[41] = (byte) 0x00;
		msgConnect[42] = (byte) 0x00;
		msgConnect[43] = (byte) 0x00;
		msgConnect[44] = (byte) 0x01;
		msgConnect[45] = (byte) 0x42;
		msgConnect[46] = (byte) 0x69;
		return msgConnect;
	}
}
