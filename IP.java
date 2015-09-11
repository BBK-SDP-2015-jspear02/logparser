package code;
/**
 * This class handles any operations related to IP address. Originally there were going to be far more.
 */
public class IP {
    private String ipAddress;

    public IP(String ipAddress){
        this.ipAddress = ipAddress;
        getIpNumber();
    }
    /**
     * @return result The ip number as a long translated from an IP Address
     */
    public long getIpNumber() {
        String[] ipAddressInArray = this.ipAddress.split("\\.");
        long result = 0;
        long ip = 0;
        for (int x = 3; x >= 0; x--) {
            ip = Long.parseLong(ipAddressInArray[3 - x]);
            result |= ip << (x << 3);
        }

        return result;
    }

}
