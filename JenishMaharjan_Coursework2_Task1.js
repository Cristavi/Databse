use JenishMaharjan_Games

db.createCollection("JenishMaharjan_10256419_Games")

db.getCollection("JenishMaharjan_10256419_Games").insertMany([
    {
     Name: "Hays Wise",
     Publisher: "KOEL Co., Ltd.",
     Released: "April 5, 1990",
     Rating: NumberInt(99),
     Country: "USA",
     Address: "694 Hewes Street",
     PlayerGoals: [
                    {
                        PlayerName: "Derrick",
                        GoalScore: NumberInt(705)
                    },
                    {
                        PlayerName: "Tim",
                        GoalScore: NumberInt(379)
                    },
                    {
                        PlayerName: "Bryan",
                        GoalScore: NumberInt(810)
                    }
                 ]
     
    },
    
     {
     Name: "Ape Escape",
     Publisher: "EIOL Co., Ltd.",
     Released: "August 5, 1990",
     Rating: NumberInt(44),
     Country: "France",
     Address: "795 Borinquen Pl",
     PlayerGoals: [
                    {
                        PlayerName: "Alpha",
                        GoalScore: NumberInt(200)
                    },
                    {
                        PlayerName: "Alan",
                        GoalScore: NumberInt(500)
                    },
                    {
                        PlayerName: "Jordan",
                        GoalScore: NumberInt(290)
                    }                 
                 ]
     
    },
    {
     Name: "Digi Laus",
     Publisher: "DIGITALUS Co., Ltd",
     Released: "September 5, 1990",
     Rating: NumberInt(50),
     Country: "Italy",
     Address: "154 Arlington Avenue",
     PlayerGoals: [
                    {
                        PlayerName: "Alpha",
                        GoalScore: NumberInt(300)
                    },
                    {
                        PlayerName: "Aubrey",
                        GoalScore: NumberInt(200)
                    }
                 ]
     
    },
    {
     Name: "Danone",
     Publisher: "Danone Co., Ltd",
     Released: "August 11, 1990",
     Rating: NumberInt(66),
     Country: "France",
     Address: "897 Borinquen Pl",
     PlayerGoals: [
                    {
                        PlayerName: "Gabriel",
                        GoalScore: NumberInt(400)
                    },
                    {
                        PlayerName: "Paul",
                        GoalScore: NumberInt(100)
                    },
                    {
                        PlayerName: "Arthur",
                        GoalScore: NumberInt(200)
                    },
                    {
                        PlayerName: "Victor",
                        GoalScore: NumberInt(120)
                    }
                 ]
     
    }    

])
    
    
/*3. Write a reduce function that calculates the total score for each team with the
    publisher name and count the number of players in each team. */


var mapFunction = function(){
    for(var i = 0; i < this.PlayerGoals.length; i++)
    {
            var key = this.Name;
            var value = {
                count: 1,
                qty: this.PlayerGoals[i].GoalScore,
                publisher: this.Publisher
                };
                emit(key, value);
    }
    }
    
var reduceFunction = function(key, value){
    reducedVal = {playercount: 0, teamtotalscore: 0, PublisherName: ""};
    
    for(var i = 0; i < value.length; i++)
    {
          reducedVal.playercount += value[i].count;
          reducedVal.teamtotalscore += value[i].qty;
          reducedVal.PublisherName = value[i].publisher;
    }
    
    return reducedVal;
    }
    
db.getCollection("JenishMaharjan_10256419_Games").mapReduce(
    mapFunction,
    reduceFunction,
    
    {
      out: "TeamNames"
    }
    ).find({});
    

//4. Count the number of players in Hays Wise.

db.getCollection("JenishMaharjan_10256419_Games").mapReduce(
        //Mapping
        function(){
            emit(this.Name, this.PlayerGoals.PlayerName)
        },
        //Reduce
        function(key, values){
            return Array.sum(values)
            },
        //Output
        {
            out: "Total Players"
        }
).find({
   
    })
    
var mapFunction = function(){
    for(var i = 0; i < this.PlayerGoals.length; i++)
    {
            var key = this.Name;
            var value = {
                count: 1,
                
                
                };
                emit(key, value);
    }
    }
    
var reduceFunction = function(key, value){
    reducedVal = {playercount: 0};
    
    for(var i = 0; i < value.length; i++)
    {
          reducedVal.playercount += value[i].count;
          
    }
    
    return reducedVal;
    }

db.getCollection("JenishMaharjan_10256419_Games").mapReduce(
    mapFunction,
    reduceFunction,
    
    {
      out: "PlayerCount_Of_HaysWise"
    }
    ).find({
       _id: "Hays Wise"
        });
        
//5. Remove the player “Alpha" from Ape Escape.
        
db.getCollection("JenishMaharjan_10256419_Games").update({
    Name: "Ape Escape"
    }, { $pull: {"PlayerGoals": {"PlayerName": "Alpha"}
    }},
        {multi: true}
    );
        
//6. Update player name “Jordan” to “Michael” and score to 300.
     
db.getCollection("JenishMaharjan_10256419_Games").update({
    "PlayerGoals.PlayerName": "Jordan"
    },
    { $set: {
                "PlayerGoals.$.PlayerName": "Michael",
                "PlayerGoals.$.GoalScore": NumberInt(300)
            }
       })

   
//7. Show all the number of players with their publisher name.
var mapFunction = function(){
    for(var i = 0; i < this.PlayerGoals.length; i++)
    {
            var key = this.Publisher;
            var value = {
                count: 1,   
                };
                emit(key, value);
    }
    }
    
var reduceFunction = function(key, value){
    reducedVal = {PlayerCount: 0 };
    
    for(var i = 0; i < value.length; i++)
    {
          reducedVal.PlayerCount += value[i].count;
          
    }
    
    return reducedVal;
    }
    
db.getCollection("JenishMaharjan_10256419_Games").mapReduce(
    mapFunction,
    reduceFunction,
    
    {
      out: "PlayerCounts_of_Publishers"
    }
    ).find({});       
       
//8. Show total goals scored by each country name.

var mapFunction = function(){
    for(var i = 0; i < this.PlayerGoals.length; i++)
    {
            var key = this.Country;
            var value = {
                
                qty: this.PlayerGoals[i].GoalScore,
                
                };
                emit(key, value);
    }
    }
    
var reduceFunction = function(key, value){
    reducedVal = { teamtotalscore: 0 };
    
    for(var i = 0; i < value.length; i++)
    {
          reducedVal.teamtotalscore += value[i].qty;
    }
    
    return reducedVal;
    }
    
db.getCollection("JenishMaharjan_10256419_Games").mapReduce(
    mapFunction,
    reduceFunction,
    
    {
      out: "TotalGoals_With_CountryNames"
    }
    ).find({});
