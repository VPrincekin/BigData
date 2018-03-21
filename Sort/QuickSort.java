package wdh.sort;

public class QuickSort {
	/**快速排序
	 *	最坏 O(n^2)
	 * 	最好 O(log(n))
	 * 	平均 O(nlog(n) )
	 * */
	public static void main(String[] args) {
		int[] array={4,5,2,3,6,1,7,9,8};
		quickSort(array, 0, array.length-1);
		for(int i : array){
			System.out.print(i);
		}
	}
	
	public static void quickSort(int[] array,int left, int right){
		if(left<right){
		int i=left,j=right;
		int x=array[i];//array[i]第一个坑
			while(i<j){
				//从右向左找小于x的数来填array[i]
				while(i<j && array[j]>=x){
					j--;
				}
				if(i<j){
					//将array[j]填到array[i]中，array[j]就形成了一个坑。
					array[i]=array[j];
					i++;
				}
				//从左向右找大于或等于x的数来填array[j]
				while(i<j && array[i]<x){
					i++;
				}
				if(i<j){
					//将array[i]填到array[j]中，array[i]就形成了一个坑。
					array[j]=array[i];
					j--;
				}
			}
			//退出时，i==j，将x填到这个坑中
			array[i]=x;
			quickSort(array, left, i-1);
			quickSort(array, i+1, right);
		}
	}
}
