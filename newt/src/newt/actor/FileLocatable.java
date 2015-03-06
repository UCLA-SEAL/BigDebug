package newt.actor;

import java.io.*;

public class FileLocatable implements LocatableDataType<FileLocatable, String> {
    static String   separator = "!";
    static int      maxFileLocatableSize = 255;

    String  directory = null;
    String  filename = null;
    long    chunkOffsetInFile = -1;
    long    dataBeginOffset = -1;
    long    dataEndOffset = -1;
    long    dataLength = -1;
    boolean isFile = true;
    boolean isFileLocation = false;

    public FileLocatable( String pathname, boolean isFile )
    {
        this( (isFile ? getParentDirectory( pathname ) : pathname), (isFile ? getLastComponent( pathname ) : null ) );
    }

    public FileLocatable( String directory, String filename )
    {
        if( directory != null ) {
            this.directory = directory.trim();
        }
        if( filename != null ) {
            this.filename = filename.trim();
        }
    }

    public FileLocatable( String pathname, boolean isFile, long dataBeginOffset, long dataLength )
    {
        this( pathname, isFile );
        this.dataBeginOffset = dataBeginOffset;
        this.dataLength = dataLength;
        this.dataEndOffset = dataBeginOffset + dataLength;
        this.chunkOffsetInFile = 0;
        this.isFileLocation = true;
    }

    public FileLocatable( String directory, String filename, long dataBeginOffset, long dataLength )
    {
        this( directory, filename );
        this.dataBeginOffset = dataBeginOffset;
        this.dataLength = dataLength;
        this.dataEndOffset = dataBeginOffset + dataLength;
        this.chunkOffsetInFile = 0;
        this.isFileLocation = true;
    }

    public FileLocatable( String pathname, boolean isFile, long dataBeginOffset, long dataEndOffsetOrLength, boolean isLength )
    {
        this( pathname, isFile, dataBeginOffset, (isLength ? dataEndOffsetOrLength : dataEndOffsetOrLength - dataBeginOffset ) );
        this.isFileLocation = true;
    }

    public FileLocatable( String directory, String filename, long dataBeginOffset, long dataEndOffsetOrLength, boolean isLength )
    {
        this( directory, filename, dataBeginOffset, (isLength ? dataEndOffsetOrLength : dataEndOffsetOrLength - dataBeginOffset ) );
        this.isFileLocation = true;
    }

    public FileLocatable( String pathname, boolean isFile, long chunkOffsetInFile, long dataBeginOffset, long dataLength )
    {
        this( pathname, isFile, dataBeginOffset, dataLength );
        this. chunkOffsetInFile = chunkOffsetInFile;
        this.isFileLocation = true;
    }

    public FileLocatable( String directory, String filename, long chunkOffsetInFile, long dataBeginOffset, long dataLength )
    {
        this( directory, filename, dataBeginOffset, dataLength );
        this. chunkOffsetInFile = chunkOffsetInFile;
        this.isFileLocation = true;
    }

    public FileLocatable( String pathname, boolean isFile, long chunkOffsetInFile, long dataBeginOffset, long dataEndOffsetOrLength, boolean isLength )
    {
        this( pathname, isFile, chunkOffsetInFile, dataBeginOffset, (isLength ? dataEndOffsetOrLength : dataEndOffsetOrLength - dataBeginOffset ) );
        this.isFileLocation = true;
    }

    public FileLocatable( String directory, String filename, long chunkOffsetInFile, long dataBeginOffset, long dataEndOffsetOrLength, boolean isLength )
    {
        this( directory, filename, chunkOffsetInFile, dataBeginOffset, (isLength ? dataEndOffsetOrLength : dataEndOffsetOrLength - dataBeginOffset ) );
        this.isFileLocation = true;
    }

    public boolean isFileLocation()
    {
        return isFileLocation;
    }

    public String getDirectory()
    {
        return directory;
    }

    public String getFilename()
    {
        return filename;
    }

    public long getChunkOffsetInFile()
    {
        return chunkOffsetInFile;
    }

    public long getDataBeginOffset()
    {
        return dataBeginOffset;
    }

    public long getDataLength()
    {
        return dataLength;
    }

    public long getDataEndOffset()
    {
        return dataEndOffset;
    }

    public String getPathname()
    {
        if( directory == null ) {
            return filename;
        } else if( filename == null ) {
            return directory;
        } else {
            return directory + '/' + filename;
        }
    }

    public String getOffsetString()
    {
        if( chunkOffsetInFile == -1 && dataBeginOffset == -1 && dataLength == -1 ) {
            return "";
        } else {
            StringBuffer sb = new StringBuffer( maxFileLocatableSize );
            sb.append( chunkOffsetInFile + dataBeginOffset );
            sb.append( separator );
            sb.append( dataLength );
            sb.append( separator );
            return sb.toString();
        }
    }

    public String toProvenance()
    {
        return toString();
    }

    public String toString()
    {
        if( filename != null ) {
            StringBuffer sb = new StringBuffer( maxFileLocatableSize );
            if( chunkOffsetInFile != -1 || dataBeginOffset != -1 || dataLength != -1 ) {
                sb.append( chunkOffsetInFile );
                sb.append( separator );
                sb.append( dataBeginOffset );
                sb.append( separator );
                sb.append( dataLength );
                sb.append( separator );
            }
            sb.append( directory == null ? "" : directory );
            sb.append( separator );
            sb.append( filename );
            return sb.toString();
        } else {
            return (directory == null ? "" : directory); 
        }
    }

    public byte[] getBytes()
    {
        return toProvenance().getBytes();
    }

    public int overlaps( LocatableDataType o )
    throws ClassCastException
    {
        if( !( o instanceof FileLocatable ) ) {
            throw new ClassCastException( "Not a FileLocatable object" );
        }

        FileLocatable l = (FileLocatable) o;
        if( l.getPathname().equals( getPathname() ) ) {
            long lChunkOffsetInFile = l.getChunkOffsetInFile();
            long lDataBeginOffset = l.getDataBeginOffset();
            long lDataEndOffset = l.getDataEndOffset();
            if( chunkOffsetInFile != -1 && dataBeginOffset != -1 && dataLength != -1 
                && lChunkOffsetInFile != -1 && lDataBeginOffset != -1 && lDataEndOffset != -1 ) {
                long lDataBegin = lChunkOffsetInFile + lDataBeginOffset;
                long dataBegin = chunkOffsetInFile + dataBeginOffset;
                long lDataEnd = lChunkOffsetInFile + lDataEndOffset;
                long dataEnd = chunkOffsetInFile + dataEndOffset;
                if( lDataBegin < dataEnd && dataBegin < lDataEnd ) {
                    return 0;
                } else if( lDataBegin >= dataEnd ) {
                    return -1;
                } else {
                    return 1;
                }
            } else {
                return 0;
            }
        } else {
            return getPathname().compareTo( l.getPathname() );
        }
    }

    public boolean equals( FileLocatable l )
    {
        if( l == null ) {
            return false;
        }

        if( l.toString().equals( toString() ) ) {
            return true;
        }

        return false;
    }

    public int compareTo( FileLocatable l )
    {
        if( l == null ) {
            throw new NullPointerException();
        }

        if( l.toString().equals( toString() ) ) {
            return 0;
        } else if( l.getPathname().equals( getPathname() ) ) {
            long lChunkOffsetInFile = l.getChunkOffsetInFile();
            long lDataBeginOffset = l.getDataBeginOffset();
            long lDataEndOffset = l.getDataEndOffset();
            if( chunkOffsetInFile != -1 && dataBeginOffset != -1 && dataLength != -1 
                && lChunkOffsetInFile != -1 && lDataBeginOffset != -1 && lDataEndOffset != -1 ) {
                long lDataBegin = lChunkOffsetInFile + lDataBeginOffset;
                long dataBegin = chunkOffsetInFile + dataBeginOffset;
                long lDataEnd = lChunkOffsetInFile + lDataEndOffset;
                long dataEnd = chunkOffsetInFile + dataEndOffset;
                if( dataBegin == lDataBegin ) {
                    if( dataEnd == lDataEnd ) {
                        return 0;
                    } else if( dataEnd > lDataEnd ) {
                        return 1;
                    } else {
                        return -1;
                    }
                } else if( dataBegin > lDataBegin ) {
                    return 1;
                } else {
                    return -1;
                }
            } else {
                return 0;
            }
        } else {
            return getPathname().compareTo( l.getPathname() );
        }
    }

    public static String getParentDirectory( String pathname )
    {
        int parentDirComponentEndIndex = pathname.lastIndexOf( '/' );
        if( parentDirComponentEndIndex <= 0 ) {
            return null;
        }
        return pathname.substring( 0, parentDirComponentEndIndex ).trim();
    }

    public static String getLastComponent( String pathname )
    {
        int lastComponentBeginIndex = pathname.lastIndexOf( '/' ) + 1;
        if( lastComponentBeginIndex == pathname.length() ) {
            return null;
        }
        return pathname.substring( lastComponentBeginIndex ).trim();
    }

    public static FileLocatable resolve( String fileLocatableString )
    {
        String[] components = fileLocatableString.split( separator );
        switch( components.length ) {
            case 1: return new FileLocatable( components[ 0 ], null );
            case 2: return new FileLocatable( components[ 0 ], components[ 1 ] );
            case 5: return new FileLocatable( components[ 3 ], components[ 4 ],
                                              Long.parseLong( components[ 0 ] ),
                                              Long.parseLong( components[ 1 ] ),
                                              Long.parseLong( components[ 2 ] ) );
            default: return null;
        }
    }

    public static String getPathname( String fileLocatableString )
    {
        FileLocatable fileLocatable = resolve( fileLocatableString );
        return (fileLocatable == null ? null : fileLocatable.getPathname() );
    }
}
