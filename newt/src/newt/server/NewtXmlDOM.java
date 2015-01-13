package newt.server;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.xerces.parsers.DOMParser;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.InputSource;

/** Parses and prints content of test.xml file.
 ** The result should be identical to the input except for the whitespace.
 */
public class NewtXmlDOM {
    Document doc = null;

    public String printXML( Node node )
    throws IOException
    {
        StringWriter str = new StringWriter();

        printWithFormat( node, str, 0, true );
        str.flush();

        return str.toString();
    }

    public Node root( Document p )
    throws IOException
    {
        return p.getDocumentElement();
    }

//    public Document doc( String fileName ) throws IOException
//    {
//        Node n = null;
//        try {
//            DOMParser p = new DOMParser();
//            p.parse(new InputSource(new StringReader(fileName)));
//            doc = p.getDocument();
//            n = doc.getDocumentElement();
//        } catch( Exception e) { 
//            e.printStackTrace();
//        }   
//    
//        return doc;
//    }
    
    public Document doc( String fileName )
    throws IOException
    {
        Document doc = null;
        Node n = null;
        try {
            DOMParser p = new DOMParser();
            p.parse( fileName );
            doc = p.getDocument();
            n = doc.getDocumentElement();
        } catch( Exception e) { 
            e.printStackTrace();
        }   
    
        return doc;
    }    
    
    public Document doc( File fileName ) throws IOException
    {
        Node n = null;
        try {
            DOMParser p = new DOMParser();
            p.parse(fileName.getAbsolutePath());
            doc = p.getDocument();
            n = doc.getDocumentElement();
        } catch( Exception e) { 
            e.printStackTrace();
        }   
    
        return doc;
    }

    public ArrayList<Node> children( Node n )
    {
        NodeList nodeList = null;
        ArrayList<Node> results = null;

        if( n.hasChildNodes() ) {
            nodeList = n.getChildNodes();
            results = sanitizeChildren( n, nodeList );
        }

        return results;
    }

    public ArrayList<Node> parent( Node n )
    {
        ArrayList<Node> results = new ArrayList<Node>();
        Node p = n.getParentNode();
        results.add( p );

        return results;
    }

    public String tag( Element n )
    {
        return n.getNodeName();
    }

    public String text( Text n )
    {
        return n.getNodeValue().trim();
    }

    public ArrayList<Node> childrenByTag( Node n, String tagName )
    {
        ArrayList<Node> nodes = null;
        ArrayList<Node> results = new ArrayList<Node>();
        if( n.hasChildNodes() ) {
            nodes = children( n );
            for( int i = 0; i < nodes.size(); i++ ) {
                Node child = nodes.get( i );
                if( child.getNodeType() == Node.ELEMENT_NODE && child.getNodeName().equals( tagName ) == true ) {
                    results.add( child );
                }
            }
        }

        return results;
    }

    public ArrayList<Node> descendantsByTag( Node n, String tagName )
    {
        NodeList descendants = null;
        ArrayList<Node> results = new ArrayList<Node>();
        Element e = ( Element ) n;
        descendants = e.getElementsByTagName( tagName );

        for( int i = 0; i< descendants.getLength(); i++ ){
            results.add( descendants.item( i ) );
        }

        return results;
    }

    public boolean areEqual( Node n1, Node n2 )
    {
        boolean result = true;

        if( n1.getNodeType() ==  n2.getNodeType() ) {
            if( n1.getNodeName().equals( n2.getNodeName() ) ) {
                if( n1.getNodeType() == Node.TEXT_NODE ) {
                    if( !( n1.getNodeValue().equals( n2.getNodeValue() ) ) ) {
                        result = false;
                    }
                } else {

                    ArrayList<Node> al1 = children( n1 );
                    ArrayList<Node> al2 = children( n2 );

                    if( al1.size() == al2.size() ) {
                        for( int i = 0; i < al1.size(); i++ ) {
                            if( areEqual( al1.get( i ), al2.get( i ) ) != true ) {
                                result = false;
                            }
                        }
                    } else {
                        result = false;
                    }
                }
            } else {
                result = false;
            }
        } else {
            result = false;
        }

        return result;
    }

    public Element makeElem( String tagName, ArrayList<Node> nodes )
    {
        if( nodes == null || nodes.size() == 0 ) {
            return null;
        }

        Element root = doc.createElement( tagName );

        for( int i = 0; i < nodes.size(); i++ ) {
            Node n = buildTree( nodes.get( i ) );
            root.appendChild( n );
        }

        Node result = root.cloneNode( true );
        return (Element)result;
    }

    public Node buildTree( Node n ) 
    {
        Node newNode = null;
        switch( n.getNodeType() ) {
            case Node.ELEMENT_NODE: 
                newNode = doc.createElement( n.getNodeName() );
                NodeList children = n.getChildNodes();
                if( children != null && children.getLength() > 0 ) {
                    for( int i = 0 ; i < children.getLength(); i++ ) {
                        newNode.appendChild( buildTree( children.item( i ) ) );
                    }
                }
                return newNode;
            case Node.TEXT_NODE:
                newNode = doc.createTextNode( n.getNodeValue().trim() );
        }
        return newNode;
    }

    public Text makeText( String text, Node docRoot )
    {
        //Document doc = docRoot.getOwnerDocument();
        return doc.createTextNode( text );
    }

    public void printNodeArray( ArrayList<Node> nodes )
    {
        for( int i = 0 ; i < nodes.size(); i++ ) {
            Node member = nodes.get( i );
            switch( member.getNodeType() ) {
                case Node.ELEMENT_NODE:
                {
                   System.out.println( "[" + member.getNodeName() + "]" );
                }
                break;

                case Node.TEXT_NODE:
                {
                   System.out.println( "[" + member.getNodeValue() + "]" );
                }
                break;

                default:
                   System.out.println( "Can't parse this node" );
            }
        }
    }

    public Node getAttribute( Node n, String attrName )
    {
        NamedNodeMap attrs = n.getAttributes();
        Node attr = null;
        
        if( attrs != null ) {
            for( int i = 0; i < attrs.getLength(); i++ ) {
                attr = attrs.item( i );
                if( attr.getNodeName().equals( attrName ) ) {
                    return attr;
                }
            }
        }

        return null;
    }
    
    public List<String> getTextByElementTag(String tag)
    {
    	List<String> result = new ArrayList<String>();
    
    	NodeList elements = doc.getElementsByTagName(tag);
    	for(int i=0; i<elements.getLength(); i++)
    	{
    		Node v = getAttribute(elements.item(i), "value");
    		result.add(v.getNodeValue());
    	}
    	return result;
    }

    private ArrayList<Node> sanitizeChildren( Node n, NodeList children )
    {
        ArrayList<Node> _children = new ArrayList<Node>();
        if( children != null && children.getLength() > 0 ) {
            for( int i = 0 ; i < children.getLength(); i++ ) {
                Node child = children.item( i );

                switch( child.getNodeType() ) {
                    case Node.ELEMENT_NODE: {}
                    break;

                    case Node.TEXT_NODE:
                    {
                        String text = child.getNodeValue().trim();
                        if( text.length() <= 0 ) {
                            n.removeChild( child );
                        }
                    }
                    break;

                    default:
                }
            }
        }

        children = n.getChildNodes();
        for( int i = 0; i < children.getLength(); i++ ) {
            _children.add( children.item( i ) );
        }

        return _children;
    }

    private final void printWithFormat( Node node, Writer wr, int n, boolean flush )
    throws IOException
    {
        switch( node.getNodeType() ) {
            case Node.ELEMENT_NODE:
            {
                // Print opening tag
                wr.write( makeTabs( n ) + "<" + node.getNodeName() );
                if( flush ) {
                    wr.flush();
                }

                // Print attributes (if any)
                NamedNodeMap attrs = node.getAttributes();
                Node attr = null;
                if( attrs != null ) {
                    for( int i = 0; i < attrs.getLength(); i++ ) {
                        attr = attrs.item( i );
                        wr.write( " " + attr.getNodeName() + "=\"" + attr.getNodeValue() + "\" " );
                        if( flush ) {
                            wr.flush();
                        }
                    }
                }

                wr.write( ">\n" );
                if( flush ) {
                    wr.flush();
                }

                // recursively print children
                Node ch = node.getFirstChild();
                while( ch != null ) {
                    printWithFormat( ch, wr, n+1, flush ); 
                    if( flush ) {
                        wr.flush();
                    }

                    ch = (Node)ch.getNextSibling();
                }

                wr.write( makeTabs( n ) + "</" + node.getNodeName() + ">\n" );
                if( flush ) {
                    wr.flush();
                }
            }
            break;

            case Node.TEXT_NODE:
            {
                String text = node.getNodeValue().trim();
                // Make sure we don't print whitespace
                if( text.length() > 0 ) {
                    wr.write( makeTabs( n ) + text + "\n" ); 
                    if( flush ) {
                        wr.flush();
                    }
                }
            }
            break;

            default:
                throw new IOException( "Cannot print this type of element" );
        }
    }

    private final String makeTabs( int n )
    {
        StringBuffer result = new StringBuffer( "" );
        for( int i = 0; i < n; i++ ) {
            result.append( "\t" );
        }
        return result.toString();
    }
}
