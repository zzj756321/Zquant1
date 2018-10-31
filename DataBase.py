import pandas as pd111
class DataBase():
    database = pd.DataFrame()
    filename =""
    def __init__(self,filename):
        ''''
        类型: 函数
        输入接口：
        DataBase(filename)
        功能及用法描述：
        filename为数据库Dataframe的存储位置。
        '''
        try:
            dataBase = pd.read_pickle(filename)
        except:
            print("文件不存在，新建文件中·····")
            database = pd.DataFrame()
            database.to_pickle(filename)
        try:
            database = pd.read_pickle(filename)
        except:
            print("创建失败")
        else:
            #print "数据库读取成功"
            self.database = database
            self.filename = filename
        return
    def updata(self,index,label,data):
        '''
        类型: 函数
        输入接口：
        updata(index,label,data)
        功能及用法描述：
        updata(index,label,data)，index为更新的索引，label为被更新内容的类别，如果类别不存在会在数据库中为所有索引内容添加
        '''
        data2 = pd.DataFrame(data)
        database = self.database
        flag = False
        if len(database.index)==0:
            self.database = data2
            return
        label2 = []
        label1 = []
        if label ==[]:
            label = list(data2.columns)
        for ix in index:
            if ix in label:
                label.remove(ix)
        for columns in label:
            if columns in database.columns:
                label1.append(columns)
                continue
            else:
                label2.append(columns)
        if len(label2)>0:
            data = data2[label2+index]
            database = database.merge(data,on=index,how='outer')
            database = database.sort_values(by=index,ascending=True)
            database = database.set_index(index)
            data = data.sort_values(by=index,ascending=True)
            data = data.set_index(index)
            database.update(data)
            database = database.reset_index()
        if len(label1)>0:
            data = data2[label1+index]
            database = database.sort_values(by=index,ascending=True)
            database = database.set_index(index)
            data = data.sort_values(by=index,ascending=True)
            data = data.set_index(index)
            database.update(data)
            database = database.reset_index()
            data = data.reset_index()
            database = database.merge(data,on=index+label1,how='outer')
        self.database = database
        return
    def save(self,filename=[]):
        ''''
        类型: 函数
        输入接口：
        save(filename=[])
        功能及用法描述：
        filename为空时，表示覆盖原位置。
        '''
        if filename==[]:
            filename = self.filename
        try:
            self.database.to_pickle(filename)
        except:
            print('保存失败')
        else:
            pass
            #print '保存成功'
        return
    def clear(self,proper=[]):
        ''''
        类型: 函数
        输入接口：
        clear(proper=[])
        功能及用法描述：
        proper为要删除的属性名称，为空时表示清空整个数据库
        '''
        if proper==[]:
            self.database = pd.DataFrame()
        else:
            if isinstance(proper,str):
                if proper in self.database.columns:
                    del self.database[proper]
                else:
                    print('不存在'+proper+'属性')
            else:
                for col in proper:
                    if col in self.database.columns:
                        del self.database[col]
                    else:
                        print('不存在'+col+'属性')
        return
    def __getitem__(self,signal):
        return self.database[signal]
    def NULL(self):
        NULL = pd.DataFrame()
        for col in self.database:
            NULL= NULL.append(self.database[self.database[col].isnull()])
        return NULL