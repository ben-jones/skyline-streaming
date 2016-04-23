def readFile():

    list3 = [[50,3.0], [51,5.0], [52,4.0], [53,2.0],[65,1.0],[25,100.0],[26,95.0],[49,4.0],[51,2.0],[31,67]];
    numOfDim = len(list3[0]);
    SkylineBasic(list3);


def SkylineBasic(inputFile):
    window = [];
    tempFile = [];
    window_size = 5;
    skylinePts = [];
    nonSkylinePts = [];
    time = 0;
    skyLinePts = skylineBNL(inputFile,window,tempFile,window_size,skylinePts,nonSkylinePts,time);
    print "The skyline points are:"
    for point in skyLinePts:
        print point;
       
    

class Tuple:
    def __init__(self,dataPoint,timeStamp):
      self.data = dataPoint;
      self.timeStamp =  timeStamp;
    
    
def skylineBNL(inputFile,window,tempFile,window_size,skylinePts,nonSkylinePts,time):
    
    
    #handle the end cases
    curIndex = 0;
    spaceInWindow = window_size;
    dominate = -1 # Dominates 1; Neutral 0; Doesn't Dominate -1
    curTuple = None;
    
    while (len(inputFile)!=0):
        
        #compare the input tuple with all the tuples in the window
        
        if(isinstance(inputFile[0],Tuple)):
            print inputFile[0].data;
        
        if(isinstance(inputFile[0],list)):
            curTuple = Tuple(inputFile[0],time);
        else:
            curTuple = inputFile[0];
            curTuple.timeStamp = time;
    
        time=time+1;
        inputFile.remove(inputFile[0]);
        
        if(len(window)==0):
            window.append(curTuple);
        else:
            windowIndex = 0;
            dominates =neutral=dominated=0;
        
            while (windowIndex<len(window)):
                result = compare(curTuple.data,window[windowIndex].data);
                #if result is 1, remove the tuple from the window;
                #if result is -1, place the curTuple in the Non-skyline;
                #if result is 0, place the curTuple in the 1) window, if place is there else in the temp file.
                if(result == -1):
                    nonSkylinePts.append(curTuple.data);
                    dominated=1;
                    break;
                elif(result == 1):
                    window.remove(window[windowIndex]);
                    dominates = 1;
                else:
                    neutral=1;
                
                windowIndex=windowIndex+1;
            
            if(dominated == 0 and dominates==1 and (neutral==0 or neutral==1)):
                
                
                if(len(window)<window_size):
                    #add the curTuple to the window;
                    window.append(curTuple);
                else:
                    #add the curTuple to the tempFile;
                    tempFile.append(curTuple);
            elif(dominated == 0 and (neutral==1)):
                        
                if(len(window)<window_size):
                    #add the curTuple to the window;
                    window.append(curTuple);
                else:
                    #add the curTuple to the tempFile;
                    tempFile.append(curTuple);
            
        
        if(len(inputFile)==0):
            #copy the tempFile into inputFile for next iteration;
            #re-initialize the tempFile for next iteration;
            if(len(tempFile)>0):
                smallest_time = tempFile[0].timeStamp;
                index =0;
                while(index<len(window)):
                    point = window[index];
                    if(point.timeStamp < smallest_time):
                        #place it in skyline and remove from window;
                        skylinePts.append(point.data);
                        window.remove(point);
                    
                    else:
                        point.timeStamp =0;
                        index = index+1;        
            else:
                
                while(len(window)!=0):
                    point = window[0];
                    skylinePts.append(point.data);
                    window.remove(point);
                
            inputFile = tempFile;
            for point in inputFile:
                print point.data;
               
            tempFile = [];
            time=0;
              
        if(len(inputFile)==0 and len(tempFile)==0):
            return skylinePts;
            
        
        
         
    
def compare(tuple1,tuple2):
    
    dimLength = len(tuple1);
    dominates = neutral = dominated = 0;
    index = 0;
    while (index < dimLength):
        if (tuple1[index] > tuple2[index]):
            dominated = 1;
        elif (tuple1[index] < tuple2[index]):
            dominates = 1;
        else:
            neutral =1;
            
        index = index+1
        
    if ((dominates==1) and ((neutral==1) or (neutral==0)) and dominated==0):
        return 1;
    elif (dominates==0 and ((neutral==1)or(neutral==0)) and dominated==1):
        return -1;
    else:
        return 0;
    
    return 0;
    
            
         
readFile();    