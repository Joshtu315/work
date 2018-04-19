class Wallet():
    def __init__(self, x):
        self.money=x
    def add(self, x):
        self.money+=x
    def view(self):
        print(self.money)

w=Wallet(100)
w.add(5)
w.view()

w.add(3)
w.view()



##########################################################################
# 1. two sum
# solution 1
class Solution(object):
    def twoSum(self,arr,targ):
        for m,x in enumerate(arr):
            for n,y in enumerate(arr):
                if arr[m] == targ - arr[n] and m!=n:
                    return  [m,n]
                    break

# solution 2
class Solution(object):
    def twoSum(self,arr,targ):
        for m,x in enumerate(arr):
            y = targ - arr[m]
            if y in arr and m!=arr.index(y):
                return  [m,arr.index(y)]
                break


##########################################################################
# 7. Reverse Integer
# solution 1 (didn't pass)
class Solution(object):
    def reverse(self,s):
        rev = ""
        if s < -(2**31) or s > 2**31 - 1:
            rev = 0
        else:
            if s < 0:
                s=str(s*-1)
                for i in s:
                    rev = i + rev
                rev=int(rev)*-1
            else:
                s=str(s)
                for i in s:
                    rev = i + rev
                rev=int(rev)
        return rev

# solution 2
class Solution(object):
    def reverse(self, x):
        s = cmp(x,0)
        r = int(str(x*s)[::-1])
        return s*r * (r < 2**31)


##########################################################################
# 9. Palindrome Number
class Solution(object):
    def isPalindrome(self,x):
        if x < 0:
            return False
        else:
            if x - int(str(x)[::-1]) == 0:
                return True
            else:
                return False


##########################################################################
# 13. Roman to Integer
class Solution(object):
    def romanToInt(self, s):
            """
            :type s: str
            :rtype: int
            """
            _dict = {'I':1,'V':5,'X':10,'L':50,'C':100,'D':500,'M':1000}
            prev = 0
            sum = 0
            for i in s[::-1]:
                curr = _dict[i]
                if prev > curr:
                    sum -= curr
                else:
                    sum += curr
                prev = curr
            return sum




##########################################################################
# 14. Longest Common Prefix
class Solution(object):
    def longestCommonPrefix(self, strs):
        """
        :type strs: List[str]
        :rtype: str
        """
        if not strs:
            return ""
        shortest = min(strs,key=len)
        for i, ch in enumerate(shortest):
            for other in strs:
                if other[i] != ch:
                    return shortest[:i]
        return shortest 


##########################################################################
# 20. Valid Parentheses
class Solution(object):
    def isValid(self, s):
        stack = []
        dict = {"]":"[", "}":"{", ")":"("}
        for char in s:
            if char in dict.values():
                stack.append(char)
            elif char in dict.keys():
                if stack == [] or dict[char] != stack.pop():
                    return False
            else:
                return False
        return stack == []



##########################################################################
# 21. Merge Two Sorted Lists
class Solution(object):
    def mergeTwoLists(self, l1, l2):
        dummy = cur = ListNode(0)
        while l1 and l2:
            if l1.val < l2.val:
                cur.next = l1
                l1 = l1.next
            else:
                cur.next = l2
                l2 = l2.next
            cur = cur.next
        cur.next = l1 or l2
        return dummy.next


##########################################################################
# 26. Remove Duplicates from Sorted Array
class Solution(object):
    def removeDuplicates(self,A):
        if not A:
            return 0 
        newTail=0
        for i in range(1, len(A)):
            if A[i] != A[newTail]:
                newTail += 1
                A[newTail] = A[i]
        return newTail + 1
    

##########################################################################
# 27. Remove Element
class Solution:
    def removeElement(self, nums, val): 
        while val in nums:
            nums.remove(val)


##########################################################################
# 28. Implement strStr()
class Solution:
    def strStr(self, haystack, needle):
        try:
            return(haystack.index(needle))
        except:
            return(-1)


##########################################################################
# 35. Search Insert Position
class Solution:
    def searchInsert(self, nums, target):
        for i in nums:
            if i >= target:
                return nums.index(i)
        return len(nums)



##########################################################################
# 53. Maximum Subarray
class Solution:
    def maxSubArray(self, A):
        if not A:
            return 0
        curSum = maxSum = A[0]
        for num in A[1:]:
            curSum = max(num, curSum + num)
            maxSum = max(maxSum, curSum)

        return maxSum



##########################################################################
# 58. Length of Last Word
class Solution(object):
    def lengthOfLastWord(self, s):
        retrun len(s.strip().split(" ")[-1])


##########################################################################
# 66. Plus One
class Solution(object):
    def plusOne(self, digits):
        n=0
        for i, ch in enumerate(digits):
            n+=(ch*(10**(len(digits)-1-i)))
        return([int(i) for i in str(n+1)])



##########################################################################
# 69. Sqrt(x)
class Solution(object):
    def mySqrt(self, x):
        r = x
        while r*r > x:
            r = (r + x/r) / 2
        return int(r)


##########################################################################
# 70. Climbing Stairs
class Solution(object):
    def climbStairs(self, n):
        if n == 1:
            return 1
        res = [0 for i in xrange(n)]
        res[0], res[1] = 1, 2
        for i in xrange(2, n):
            res[i] = res[i-1] + res[i-2]
        return res[-1]
        

##########################################################################
# 83. Remove Duplicates from Sorted List
class Solution(object):
    def deleteDuplicates(self, head):
            if head is None:
                return head
            curr = head
            while curr.next:
                if curr.next.val == curr.val:
                    curr.next = curr.next.next
                else:
                    curr = curr.next
            return head


##########################################################################
# 88. Merge Sorted Array
class Solution(object):
    def merge(self, nums1, m, nums2, n):
        """
        :type nums1: List[int]
        :type m: int
        :type nums2: List[int]
        :type n: int
        :rtype: void Do not return anything, modify nums1 in-place instead.
        """
        nums1[m:]=nums2[:n]
        return nums1.sort()


##########################################################################
#100. Same Tree
class Solution(object):
    def isSameTree(self, p, q):
        if p and q:
            return p.val == q.val and self.isSameTree(p.left, q.left) and self.isSameTree(p.right, q.right)
        return p is q


##########################################################################
# 118. Pascal's Triangle
class Solution:
    def generate(self, numRows):
        lists = []
        for i in range(numRows):
            lists.append([1]*(i+1))
            if i>1 :
                for j in range(1,i):
                    lists[i][j]=lists[i-1][j-1]+lists[i-1][j]
        return lists



##########################################################################
# 119. Pascal's Triangle II
class Solution:
    def getRow(self, rowIndex):
        lists = []
        for i in range(rowIndex+1):
            lists.append([1]*(i+1))
            if i>1 :
                for j in range(1,i):
                    lists[i][j]=lists[i-1][j-1]+lists[i-1][j]
        return lists[-1]



##########################################################################
# 121. Best Time to Buy and Sell Stock
class Solution(object):
    def maxProfit(self,prices):
        max_profit, min_price = 0, float('inf')
        for price in prices:
            min_price = min(min_price, price)
            profit = price - min_price
            max_profit = max(max_profit, profit)
        return max_profit


##########################################################################
# 122. Best Time to Buy and Sell Stock II

class Solution(object):
    def maxProfit(self, prices):
        maxpro=0
        for i in range(0,len(prices)-1):
            maxpro+=max(prices[i+1] -prices[i],0)
        return maxpro





##########################################################################
# 125. Valid Palindrome
class Solution(object):
    def isPalindrome(self, s):
        if not s:
            return True
        i = []
        for each in s:
            if each.isalnum():
                i.extend(each.lower())
        return i == i[::-1]



##########################################################################
# 136. Single Number
class Solution(object):
    def singleNumber(self, nums):
        return sum(set(nums))*2 - sum(nums)



