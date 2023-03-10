using System;

namespace Budget.TimerFunction
{
public class ConfigStore
{
    public static string AADAuthority {get; set;} 
    public static string AADClientId {get; set;} 
    public static string AADTenantId {get; set;} 
    public static string AADClientSecret {get; set;} 
    public static string DestinationStorageConn {get; set;}
    public static string DestinationContainer {get; set;}
    public static string DestinationBlobName {get; set;}
    public static string DestinationContainerHistory {get; set;}
    public static string DestinationBlobNameHistory {get; set;}
    public static DateTime StartDate {get; set;}
}
}