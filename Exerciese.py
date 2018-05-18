

###########################################################################
# Pandas
##########################################################################

# read csv 
pcm_amex=pd.read_csv("PartytoPCMAmex1.1.csv",delimiter="|")

# write to csv 
loan_null.to_csv('loan_null.csv')

# count of unique 
pcm_amex.uuid.nunique()

# group by two columns and get count 
temp=pcm_amex.groupby(['uuid', 'dt2_business']).size()

# group by and count of unique of one column
temp= pcm_amex.groupby('uuid').dt2_business.nunique()


# select certain columns when one column match specfic values and sort
pcm_amex.loc[(pcm_amex["uuid"] == "ffffe9a0-2030-48e4-a560-7db1e6a21605"),["uuid","dt2_business","amexindicator"]].sort_values(["dt2_business"], ascending=True)
pcm_amex.loc[(pcm_amex["uuid"] in ("00003e23-d400-475d-9e21-dcb05bfd0c8f","ffffe9a0-2030-48e4-a560-7db1e6a21605")),["uuid","dt2_business","amexindicator"]].sort_values(["dt2_business"], ascending=True)

test=party_null[party_null['Fund_Year']>2011]
test1=test.IDN_MS_LOAN.unique()
output=mtg_v[mtg_v['IDN_MS_LOAN'].isin(test1)]


# data frame join two tables 
mtg_party=pd.merge(mtg_new, loantoparty, left_on='IDN_MS_LOAN', right_on='idn_ms_loan', how='outer')

# sum of null missing count by group 
party_null.groupby(['Fund_Year'])['LastPaymentDate'].apply(lambda x: x.isnull().sum())

# aggregate multiple columns 
pla_aggre=pla.groupby('DT2_BUSINESS').agg({'OUTSTANDING_BALANCE':['size','mean'],'FACILITY_AMOUNT':['size','mean'],'AMT_COLLATERAL_ADVANCE':['size','mean'],'COLLATERAL_MKT_VALUE':['size','mean']})

# group by having 
dups=lal_pla_mcall.groupby(['FACILITY_KEY_ACCOUNT','MONTH_END_DATE']).size().loc[lambda x: x>1].sort_values()


# convert to string 
df[["A", "C"]] = df[["A", "C"]].astype(str) 
mtg_miss['DT2_BUSINESS'] =  pd.to_datetime(mtg_miss['DT2_BUSINESS'], format='%m/%d/%Y')


# reset index
df = df.reset_index

# round to int
5//3
# 1 

# get remianing number
5%3
# 2



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
        return len(s.strip().split(" ")[-1])


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
# solution 1 
class Solution(object):
    def singleNumber(self, nums):
        return sum(set(nums))*2 - sum(nums)

# solution 2
def singleNumber1(self, nums):
    dic = {}
    for num in nums:
        dic[num] = dic.get(num, 0)+1
    for key, val in dic.items():
        if val == 1:
            return key


##########################################################################
# 141. Linked List Cycle
class Solution(object):
    def hasCycle(self, head):
        slow = fast = head
        while fast and fast.next:
            fast = fast.next.next
            slow = slow.next
            if slow == fast:
                return True
        return False

##########################################################################
# 155. Min Stack
class MinStack(object):

    def __init__(self):
        self.stack = []
        
    def push(self, x):
        self.stack.append((x, min(self.getMin(), x))) 
           
    def pop(self):
        self.stack.pop()

    def top(self):
        if self.stack:
            return self.stack[-1][0]
        
    def getMin(self):
        if self.stack:
            return self.stack[-1][1]
        return sys.maxint      


##########################################################################
# 167. Two Sum II - Input array is sorted
class Solution(object):
    def twoSum(self, numbers, target):
        """
        :type numbers: List[int]
        :type target: int
        :rtype: List[int]
        """
        m, n = 0, len(numbers) - 1
        while m<n:
            tot=numbers[m] + numbers[n]
            if tot == target:
                return [m+1,n+1]
            elif tot < target:
                m+=1
            else:
                n-=1



##########################################################################
# 168. Excel Sheet Column Title

class Solution:
    def convertToTitle(self, n):
        result = ''
        distance = ord('A') 
        while n > 0:
            y = (n-1) % 26
            n = (n-1) // 26
            s = chr(y+distance)
            result = ''.join((s, result))
        return result


##########################################################################
# 169. Majority Element
class Solution(object):
    def majorityElement(self, nums):
        dic = {}
        for num in nums:
            dic[num] = dic.get(num,0) + 1
            if dic[num] >len(nums)/2:
                return num




##########################################################################
# 171. Excel Sheet Column Number

class Solution(object):
    def titleToNumber(self, s):
        """
        :type s: str
        :rtype: int
        """
        a = 0
        for i, x in enumerate(s[::-1]):
            a+=(ord(x)-64)*(26**i)
        return a






##########################################################################
# 172. Factorial Trailing Zeroes
class Solution(object):
    def trailingZeroes(self, n):
        num, i = 0,1
        while 5**i <= n:
            num += n//(5**i)
            i+=1
        return num




##########################################################################
# 175. Combine Two Tables
select a.FirstName,a.LastName,b.city,b.state
from Person a 
left join Address b on a.personid=b.personid 




##########################################################################
# 176. Second Highest Salary
select max(salary) as SecondHighestSalary  from Employee  where salary < (select max(salary) from Employee )



##########################################################################
# 181. Employees Earning More Than Their Managers
select a.Name as Employee   from Employee a join Employee b on a.managerid = b.id where a.salary > b.salary



##########################################################################
# 182. Duplicate Emails
select email from (
select email,count(*) as count from Person group by email having count > 1) a



##########################################################################
# 183. Customers Who Never Order
select a.name  as customers from customers a left join orders b on a.id=b.CustomerId where b.id is null 



##########################################################################
# 189. Rotate Array
class Solution(object):
    def rotate(self, nums, k):
        nums[:] = nums[len(nums)-k:] + nums[:len(nums)-k]


##########################################################################
# 601. Human Traffic of Stadium
select distinct s.id, s.date, s.people
from
stadium s,
(select
s1.id
from
stadium s1,
stadium s2,
stadium s3
where
s2.id = s1.id+1 and s3.id = s1.id+2
and
s1.people>=100 and s2.people>=100 and s3.people>=100
) ab
where
s.id between ab.id and ab.id+2


##########################################################################
# 626. Exchange Seats
SELECT CASE WHEN id%2 = 0 THEN id-1
            WHEN id%2 != 0 AND id != (SELECT MAX(id) FROM seat) THEN id+1
            WHEN id%2 !=0 AND id = (SELECT MAX(id) FROM seat) THEN id
            END AS id, 
       student
FROM seat 
ORDER BY 1




