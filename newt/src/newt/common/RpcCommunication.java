package newt.common;

import com.caucho.hessian.client.HessianProxyFactory;
import newt.contract.NewtService;
import java.net.MalformedURLException;

public class RpcCommunication {
    static HessianProxyFactory factory;

    static { 
        factory = new HessianProxyFactory();
        factory.setHessian2Reply( true );
        factory.setHessian2Request( true );
    }

    public static NewtService getService( String url ) {
        try {
            //return (NewtService)factory.create( NewtService.class, url );
            return (NewtService)factory.create( NewtService.class, url, RpcCommunication.class.getClassLoader()); 
        } 
        catch( MalformedURLException e ) {
            e.printStackTrace();
            return null;
        }
    }
}
