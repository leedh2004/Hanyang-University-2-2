#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>

int list[8] = {1,2,4,6,8,10,20,40};

int A[4000][4000], B[4000][4000], C[4000][4000];
int A_fd, B_fd, C_fd;

// 쓰레드 함수에 2개의 인자를 넘기기 위해서
struct threadData{
	int row;
	int cnt;
}; 
// 최대 40개의 쓰레드 인자를 담을 수 있는 구조체 배열
struct threadData threadDataArray[40];

void setting_file_A(char* pathname){

	int i,j;

	if ( (A_fd = open(pathname,O_RDONLY,0666)) > 0){
		//CASE : FILE EXISTS
		int read_val;

		lseek(A_fd,0,SEEK_SET);
		for(i=0;i<4000;i++){
			for(j=0;j<4000;j++){
				read(A_fd,&read_val,sizeof(int));
				A[i][j] = read_val;
			}
		}
		return ;
	}
	else{
		//CASE : MAKE NEW FILE
		//FILE FORMAT : i = row, j = column
		A_fd = open(pathname,O_RDWR|O_CREAT,0666);
		int val = 1;
		lseek(A_fd,0,SEEK_SET);

		for(i=0; i < 4000; i++){
			for(j=0; j < 4000; j++){
				A[i][j] = val;
				write(A_fd, &val, sizeof(int));
			}
		}	
	}
}

void setting_file_B(char* pathname){
	int i,j;
	if ( (B_fd = open(pathname,O_RDONLY,0666)) > 0){
		//CASE : FILE EXISTS
		int read_val;

		lseek(B_fd,0,SEEK_SET);
		for(i=0;i<4000;i++){
			for(j=0;j<4000;j++){
				read(B_fd,&read_val,sizeof(int));
				B[i][j] = read_val;
			}
		}
		return;
	}else{
		//CASE : MAKE NEW FILE
		//FILE FORMAT : i = row, j = column
		B_fd = open(pathname,O_RDWR|O_CREAT,0666);
		int val = 2;
		lseek(B_fd,0,SEEK_SET);

		for(i=0; i < 4000; i++){
			for(j=0; j < 4000; j++){
				B[i][j] = val;
				write(B_fd, &val, sizeof(int));
			}
		}	
	}
}

void setting_file_C(char* pathname){
	// C는 A와 B에 따라 바뀌므로, 싹 비워서 만들어준다.
	C_fd = open(pathname,O_RDWR|O_CREAT|O_TRUNC,0666);
}

long long int return_sum_of_C(){
	long long int sum = 0;
	int read_num,i,j;
	lseek(C_fd,0,SEEK_SET);
	for(i=0; i < 4000; i++){
		for(j=0; j < 4000; j++){
			sum += C[i][j];
		}
	}
	return sum;
}

// FORMAT : C[i][j] -> lseek(fd,(4000*4*i)+(4*j),SEEK_SET);
// i : column, j : row

void *multi (void * arg){
	struct threadData *myData;
	//sleep(1);
	myData = (struct threadData*) arg;
	int row,cnt;
	row = myData->row;
	cnt = myData->cnt;

	int i,j,k,start_point = row * cnt * 4;
	int column = 4000, sum, read_A, read_B;
	//printf("IN MULTIPLYING row: %d cnt :%d\n",row,cnt);

	for (i=0; i<row; i++){	
		for(j=0; j < column; j++){
			sum = 0;
			for(k=0; k < 4000;k++){
				sum += A[i][k] + B[k][j];
			}
			// C[i][j], start_point -> 다음 쓰레드에서 적을 위치
			C[i][j] = sum;
			lseek(C_fd,start_point+(4000*4*i)+(4*j),SEEK_SET);
			write(C_fd,&sum,sizeof(int));
		}
		//printf("%d\n",i);
	}
	pthread_exit(NULL);
}


int main(){

	int count_num = 0, thread_num, thread_cnt,i,j;
	int count_end = sizeof(list)/sizeof(int);
	clock_t before;
	double result;
	long long int sum;
	pthread_t threads[40];

	printf("setting A\n");
	setting_file_A("A.txt");
	printf("setting B\n");
	setting_file_B("B.txt");
	setting_file_C("C.txt");
	printf("setting complete\n");

	while ( count_num != count_end){
		before = clock();
		thread_num = list[count_num];

		int column = 4000;
		int row = 4000 / thread_num;
		int rc;

		for(i=0; i<thread_num; i++){
			threadDataArray[i].row = row;
			threadDataArray[i].cnt = i;
			printf("Creating threads %d\n",i);
			rc = pthread_create(&threads[i],NULL,multi,(void*)&threadDataArray[i]);			
			
			if(rc)
				printf("Creating thread error\n");
		}
		for(j=0; j<thread_num; j++){
			pthread_join(threads[j],NULL);
		}
		sum = return_sum_of_C();
		result = (double)(clock() - before) / CLOCKS_PER_SEC;
		printf("%d Threads, SUM : %lld, time : %8.3f\n",thread_num,sum,result);
		count_num ++;
	}

	return 0;
}
