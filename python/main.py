from module import connect


def is_an_adult(age: int, has_id : bool) -> bool:
    return age>= 21 and has_id

def is_bob(name: str) -> bool:
    return name.lower() == 'bob'

def enter_club(name: str, age: int, has_id : bool) -> None:
    if is_bob(name):
        print('Get out of here Bob, we don\'t want no trouble')
        return
    
    if is_an_adult(age, has_id):
        print('you may enter the club')
    else:
        print('You may not enter the club')
    

def main() -> None:
    enter_club('Bob', 29, has_id= True)
    enter_club('James', 29, has_id= True)
    enter_club('Sandra', 29, has_id= True)

def upper_everything(elements : list[str]) -> list[str]:
    return [element.upper() for element in elements]

def lower_everything(elements : list[str]) -> list[str]:
    return [element.lower() for element in elements]

loud_list : list[str] = ["mario", "james"]
# loud_list2 : list[int] = ['a', 1, 'b', 2]

if __name__ == '__main__':
    connect()
    upper_everything(loud_list)
    main()
