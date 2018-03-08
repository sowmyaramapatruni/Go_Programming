package main

import "strings"
import "os"
import "fmt"
import "strconv"
import "log"
import "encoding/json"
import "container/list"
import "time"

// structures to store JSON input and output messages

type input struct {
    T_id int
    Datafile string
    Start int64
    End   int64
}
type output struct {
    T_id int
    Value   int64
    Prefix  string
    Suffix  string
    Fragment string
}

// function to report panic

func check(e error){
  if e != nil {
     panic(e)
  }

}

// worker function and its functionality to compute partial_sum, prefix and suffix values

func worker(c chan []byte, b []byte ) {
   
    var i input
    var prefix_pos int = 0
    var prefix string = ""
    var suffix string = "" 
    var fragment string= ""
    var partial_sum int64= 0
   
    err:= json.Unmarshal(b,&i)
    check(err)

    var T_id int = i.T_id
    f, err := os.Open(i.Datafile)
    f.Seek(i.Start,0)
    bytes := make([]byte, i.End - i.Start)
    f.Read(bytes)

    nums:=string(bytes)
    var suffix_pos int = len(nums)-1

    // checks If the fragment has prefix value
    if(!strings.HasPrefix(nums," ")) {
        
       prefix_pos= strings.IndexAny(nums," ")
       if ( prefix_pos == -1 ) {
            prefix = ""
       }else{
            temp:= nums[:prefix_pos]
            prefix = string(temp)
       }
    }

    // checks If the fragment has suffix value
    if(!strings.HasSuffix(nums," ")) {
        
       suffix_pos = strings.LastIndexAny(nums," ")
       if( suffix_pos == -1) {
           suffix = ""
       }else {
           temp:= nums[suffix_pos+1:]
           suffix = string(temp)
      }
    }
    
     // handling of fragments whith no white spaces
     // It updates the fourth value in JSON message namelu Fragment

     if ((prefix == "") && (suffix == "")) {
         fragment = string(nums)
     }else {

         // computes the partial sum of the fragment
         partial_num :=nums[prefix_pos:suffix_pos]
         str:= strings.Split(partial_num, " ")

        for i:=0;i<len(str);i++ {
            if str[i] == "" {
                continue
            }
            temp,err := strconv.Atoi(string(str[i]))
            check(err)
            partial_sum = partial_sum +  int64(temp)

        }
    }

    // after all the operation is done, worker fills in the JSON message 
    // and sends it to the coordinator using channel
    // As the channel is started after all the operation is made, this ensures less elapsed time

    m :=output{T_id,partial_sum,prefix,suffix,fragment}
    out,err1 :=json.Marshal(m)
    check(err1)
    c <- out 

}


// coordinator function which spawns worker threads
func coordinator(filePath string, workers int,result chan int64){

    c := make(chan []byte, workers)
    var map_c map[int]output
    map_c = make(map[int]output)
    var sumOfPartials int64
    
    // Open the file
    f_open, err := os.Open(filePath)
    check(err)

    // to get get the size of file

    f_size,err:=f_open.Stat()
    check(err)

    size_file := f_size.Size()

    // divide the file into fragments

     size_of_fragment := size_file/int64(workers)
     rem_fragment := size_file % int64(workers)
     size_of_last_fragment := size_of_fragment + rem_fragment

     var start int64 =0
     var end int64 = size_of_fragment
     l := list.New() 
     
     for w:=0 ; w<workers; w++ {
         // special handling for last fragment 
         if w == workers-1 {
              m :=input{w,filePath,start,start+size_of_last_fragment-1}
              file_json,err :=json.Marshal(m)
              check(err)
              go worker(c,file_json)
              break
         }
         // calling workers 
         m :=input{w,filePath,start,end}
         file_json,err :=json.Marshal(m)
         check(err)

         go worker(c,file_json)
         start = end
         end   = end + size_of_fragment
     }

   // this loop waits untill all the threads are returned back
   // channels directly update the global list
   // Locking is automatically handled by channels

   for j :=0 ; j<workers; j++ {
         l.PushBack(string(<-c))
   }

   // This loop copies the list contents to map so that all the 
   // fragements are stored in sorted manner
   for temp := l.Front(); temp != nil; temp = temp.Next() {
                m := output{}
                json.Unmarshal([]byte(temp.Value.(string)), &m)
                map_c[m.T_id] =m
   }


   // This loop computes the total sum 
   // All the following cases are handled
   // 1. when the fragment contains the suffix, prefix, value
   // 2. when the fragement contains no white space
   // 3. when the fragment contains only white space
   // 4. when there is only one thread to compute the sum
   for j:=0; j<workers;j++ {
       m :=map_c[j]
       sumOfPartials+=m.Value
       if (j==0){

            // special handling when only when thread is spawned
            if (workers == 1) {
                if (m.Suffix != "") {
                      pre0,err := strconv.Atoi(m.Suffix + m.Fragment)
                      if err!=nil{
                          log.Fatal(err)
                      }
                      sumOfPartials+=int64(pre0)
                }
                if(m.Value == 0 && m.Prefix == "" && m.Suffix == "") {
                     preLast,err := strconv.Atoi(m.Fragment)
                     if err!=nil{
                         log.Fatal(err)
                      }
                     sumOfPartials+=int64(preLast)
                }

            }

            // special handling for Prefix of first thread
            if (m.Prefix != "") {
                pre0,err := strconv.Atoi(m.Prefix)
                if err!=nil{
                    log.Fatal(err)
                }
                sumOfPartials+=int64(pre0)
              }
             continue
       }
       // gets the previous fragment to calculate partial sum
       n:=map_c[j-1]
   
       // when fragment doesnot contain space
       if(m.Value == 0 && m.Prefix == "" && m.Suffix == "") {
    
            // When fragment contains only space
            if (m.Fragment == " " ) {
                 pre1,err := strconv.Atoi(n.Fragment)
                 if err!=nil{
                    log.Fatal(err)
                 }
                 sumOfPartials+=int64(pre1)
                 m.Fragment = ""
                 map_c[m.T_id] = m
                 continue
            }
            m.Fragment = n.Suffix + n.Fragment + m.Fragment
            map_c[m.T_id] = m
       }else {
           split_num:=n.Suffix+n.Fragment + m.Prefix
           if split_num == "" {
              continue
            }
            pre1,err := strconv.Atoi(split_num)

            if err!=nil{
                 log.Fatal(err)
            }
            sumOfPartials+=int64(pre1)
       }

       if m.T_id== workers-1 {

            if(m.Value == 0 && m.Prefix == "" && m.Suffix == "") {
                preLast,err := strconv.Atoi(m.Fragment)
                if err!=nil{
                  log.Fatal(err)
                 }
                 sumOfPartials+=int64(preLast)
                 break
            }
            if m.Suffix==""{
                  break
            }
            preLast,err := strconv.Atoi(m.Suffix)
            if err!=nil{
               log.Fatal(err)
            }
            sumOfPartials+=int64(preLast)
            break
      }
   }
   fmt.Println("Total sum is ",sumOfPartials);
   result<-sumOfPartials
}

func main() {

   start := time.Now()  
   no_of_workers := os.Args[1]
   filePath := os.Args[2]

   var workers int =0
   workers,err := strconv.Atoi(no_of_workers)
   check(err)

   if workers < 1 {
      fmt.Println("Minimum One thread should be spawned")
      panic("Minimum One thread should be spawned")
   }

   c:= make(chan int64)
   go coordinator(filePath,workers,c)
   <-c
   elapsed := time.Since(start)
   fmt.Println(elapsed)
}
