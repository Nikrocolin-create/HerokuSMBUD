import sys
from random import randint
from neo4j import GraphDatabase, basic_auth

from datetime import date

global conn


class Connection:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=basic_auth(user, password))
        print("connected")

    def close(self):
        self.driver.close()

    def compute_query(self, query):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, query)
            return greeting

    @staticmethod
    def _create_and_return_greeting(tx, query_text):
        result = tx.run(query_text)
        entire_result = []  # Will contain all the items
        for record in result:
            entire_result.append(record)
            entire_result.append("\n")
        return entire_result


def compute_infections(list):

    for i in range(1, randint(10, 15)):
        chosen_infected = list.pop(randint(0, len(list)-1))
        date = "datetime('" + str(randint(2019, 2022)) + "-" + str(randint(1, 12)) + "-" + str(randint(1, 28)) + "')"

        query = "MATCH (a:Person{taxCode:'"+str(chosen_infected)+"'})" \
                "CREATE (a)-[:GOT_AN]->(i:Infection{date_of_infection:"+date+"})"

        print("computing infections...")
        conn.compute_query(query)
        print("infections created")


def compute_gp(list_to_copy):
    list = list_to_copy.copy()

    type_list = ["Covid-19 Test", "Pfizer Vaccination", "Moderna Vaccination", "Johnson&Johnson Vaccination",
                 "Astrazeneca Vaccination", "Sputnik V Vaccination"]

    today = date.today()

    for numGp in range(0, randint(30, 50)):

        # GreenPass dates can vary of +-2 months (in the two worst cases) from the current date
        y1 = today.year
        m1 = today.month + randint(-1, 1)
        d1 = randint(1, 28)

        date1 = "datetime('" + str(y1) + "-" + str(m1) + "-" + str(d1) + "')"

        chosen_tax = list.pop(randint(0, len(list) - 1))
        chosen_type = type_list[randint(0, len(type_list) - 1)]

        if chosen_type == type_list[0]:
            date2 = "datetime('" + str(y1) + "-" + str(m1) + "-" + str(d1 + 2) + "')"
        else:
            date2 = "datetime('" + str(y1 + 1) + "-" + str(m1) + "-" + str(d1)+ "')"

        query = "MATCH (a:Person) where a.taxCode='" + str(chosen_tax) + "' CREATE ( (a)-[:HAS_A]->" \
                "(gp:GreenPass{date1:" + date1 + ", date2:" + date2 + ", type:'" + chosen_type + "'}) )" \
                ", ( (a)<-[:BELONGS_TO]-(gp) )"
        print("computing gps...")
        conn.compute_query(query)
        print("gp created.")


def compute_people():
    names = "Joseph Price,Mia Spencer,Leslie Hamilton,Shelly Glover,Derek Jones,David Reid,Christopher Diaz,Heather Williams,Julie Logan,Corey Mcgrath,Cheyenne Hall,Craig Green,Craig Forbes,Wanda Robertson,Patrick Riley,Peter Saunders,Robert Preston,Jill Bennett,Kim Henry,Rachel Schneider,Todd Washington,Amber Hart,Cameron Harris,David Smith,Lindsey Olson,Sara Riggs,Ruth Salazar,Philip Pierce,Kristin Sharp,Sheena Mclean,Wanda George,Denise Williams,Christopher Smith,Tanya Robertson,Russell Griffin,Lauren Lee,Karen Kim,Aaron Simon,Pamela Rice,John Santos,Samantha Day,John Wilson,Betty Butler,Kelly Horn,Aaron Kirby,Eric Ho,Patricia Morse,Perry Dougherty,Jessica Chung,April Dodson,Rebecca Carter,Joseph Smith,John Meyer,Nicole Booth,Kevin Sheppard,Melissa Clayton,Justin Adams,Rebecca Rivera,Justin Chung,Linda Sanchez,Paula Watson,Phillip Castillo,Mrs. Lydia Rojas,Anna Trujillo,Jordan Bowen,Thomas Wolfe,Kevin Mcmillan,Chad Hernandez,Scott Gross,Tamara Hale,Connie Frank,Cory Conrad,Thomas Peters,Jimmy Olson,Matthew Garza,Jeremy King,Pamela Thomas,Vanessa Taylor,Natalie Gentry,Amanda Velasquez,Melvin Sullivan,Michael Lambert,Kristopher Hopkins,Jonathan Floyd,Jennifer Peterson,Angela Bowen,Robert Harvey,Cindy Dominguez,Brian Garza,Kristy Moreno,Sherri Williams,James Watkins,Lori Munoz,Thomas Cannon,Jason Fuentes,Gloria Davis,Helen Marshall,Rhonda Wilkinson,Virginia Robles,Jacqueline Tate,Alexis Henderson,John Torres,Omar Boyd,Thomas Turner,Keith Winters,Jill Kline,Amanda Joseph,James Roach,Holly Matthews,Jennifer Ortiz,Emily Snow,Erica Ray,Ethan Terry,Jesse Wong,Joel Taylor,Kaitlyn Stone,Chelsea Bates,Caroline Santos,Kristina Johnson,Darryl Moss,Dillon Kelley,Jasmin Anderson,Randy Braun,Martin Baker,Stephen King,Marcus Wilson,Lauren Hughes,Lee Proctor,Jonathan Bautista,Carlos Houston,Melissa Hull,Pedro Neal MD,Scott Johnson,Christina Clark,Patricia Barnes,Wendy Rosario,Christine Marquez,Eric Orr,Scott Cantu,Drew Morrison,Kenneth Barker,Craig Sawyer,Sarah Lopez,Jacqueline Santana,Lisa Garcia,Miguel Townsend,Joseph Cook,Joe Williams,Lisa Martinez,Jason Myers,Richard Parker,Robert Bentley,Luis Howard,Jose Bauer,Molly Smith,Dwayne Robertson,Sara Morrison,Kenneth Dean,Rachel Dawson,Meredith Carr,Emily Sandoval,Julie Jones,Joshua Johnson,Holly Olson,Kevin Strong,Megan Rodriguez,Cassidy Duran,Ashley Mitchell,Heidi Nelson,Antonio Bell,Joseph Gonzalez,Suzanne Mccarthy,Natasha Deleon,James Hubbard,Robert Small,Dr. Carrie Beck,Bruce Cross,Kenneth Salas,Phyllis Harris,Breanna Simmons,Sheila Howard,Angela Ortiz,Jessica Moore,Stacie Larson,Gary Potts,Sandra Fuller,Allison Brown,Sherri Stevens,Michael Miller,Steven Hall,Jasmine Pope,Deborah Lin,Sharon Baker,Mason Bonilla,Denise Jones,Loretta Kim,Kelli Christian,Jennifer Robinson,Lisa Allison,Danny Cooper,Barry Robinson,Anita Morse,John Flores,Jennifer Avila,Raven Lee,Heidi White,Kimberly Lambert,Desiree Brown,Valerie Cordova,Jeffrey Miller,Leonard Henderson,Peter Mason,Aimee Rocha,Crystal Powell,Jesse Cooper,Sandra Williams DDS,Marissa Mitchell,Heather Anderson,Tony Barber,Emily Sullivan,William Mills,Garrett Edwards,Tammy Spencer,Lauren Elliott,Frederick Wagner,Russell Shaw,Karen Gonzalez,Angelica Walker,James Shelton,Cynthia Flores,Michael Daniels,Chelsea Mendoza,Cindy Grant,Tammy Baker,James Conrad,Sandra Gonzales,Debra Carter,Shawn Jacobs,Jeffrey Sparks,Jeremiah Pitts,Ruben Moreno,Pamela Hughes,Kelly Santos,Danielle Salinas,Elizabeth Garcia,Raymond Torres,Keith Cox,Mrs. Allison Peterson MD,Jacob Smith,Matthew Roberts,Candace Lewis,John Lyons,Hunter Arellano,Lisa Baldwin,Bonnie Price DDS,Timothy Dalton DDS,Eric Fisher,Carlos Mccoy,Jessica Bennett,Evan Martinez,Brenda Stewart,Dawn Walker,Jamie Vargas,Kimberly Espinoza,Christine Church,Anthony Jones,James Vargas,Shelia Frey,Andrea Allen,Natasha Jones,Peter Cole,Lauren Curtis,Perry Christensen,Charles Henderson,Olivia Suarez,Steven Floyd,Travis Navarro,Julie Fitzpatrick,Michael Ryan,Anthony Perkins,Jason Shelton,Briana Scott DDS,Patricia Mcneil,Samuel Randall,Douglas Rodriguez,Vanessa Ellis,Jordan Davis,Joyce Colon,Brian Duncan,Kimberly Gonzalez,Katrina James,Casey Willis,Charles Cervantes,John Moody,Nancy Mayo,Tyler Carroll,Roger Marks,Larry Riggs,Wendy Nunez,Nicole Robbins,James Hoffman,April Sweeney,Michelle Sanchez,John Valdez,Elizabeth White,Marcus Shepard,Lisa Lee,Lori Lopez,Barbara Perez,Kelly Malone,Casey Carter,Thomas Bailey,Ann Campbell,Lisa Park,Thomas Davidson,Kara Luna,Dustin Solis,Jeremy Morris,Patricia George,Aaron Gaines,Sarah Soto,Rebecca Cross,Jason Mcintyre,Kathleen Perez,Daniel Gonzalez,Charles Howard,Rebecca Williams,Jerry Phillips,Carrie Cooper,Clinton Johnson,Rachel Moore,Melanie Cox DDS,Angela Mcdonald,Christy Jensen,Karen Duncan,William Young,Leah Mora,Gregory Hickman,Colleen Smith,Brandon Bates,Jennifer Hicks,Tara Nguyen,Megan Hall,Rachel Clark,Melissa Scott,Summer Little,Tammy Serrano,Michael Williams,Angela Waters,John Paul,Nicholas Greene,Johnny Casey,Jennifer Robertson,Pamela Smith,Kevin Cunningham,Amber Howard,Mercedes Douglas DVM,Ashley Nash,Christopher Garcia,Angela Rodriguez,Pamela Booth,Gregory Kirk,Charlotte Salinas,Shannon Martin,Joshua Crane,Anne Reyes,Samantha Marshall,Christine Clark,Norma Cox,John Chapman,Aaron Smith,Mr. Richard Taylor MD,Lauren Hicks,Katherine Kelley,Chase Sullivan,Stacey Olson,April Jones,Tamara Hernandez,Joshua Gentry,Michael Patterson,Amy Thomas,James Kelley,Tracy Weber,Mrs. Carla Hughes,Scott Smith,William Garcia,Christopher Willis,Randy Hernandez,Dylan Clayton,Vickie Delgado,Richard Rangel,Carl Foster,Julie Flores,Heather Fields,Brandon Campos,Kimberly Wood,Kaitlyn Mcmillan,Kelsey Allen,Eric Long,Ann Gibson,Stephanie Ortiz,Gregory Holmes,Mary Johnson,Heather Lowery,Ashley Johnson,Debra Mendez,Barbara Hutchinson,Ryan Montes,Alison Oliver,Tiffany Murray,Emily Diaz,Aaron Harrison,Crystal Parker,James Henry,Michelle Summers,Terry Wilson,David Page,Joanna Jackson,Gabriela Williams,John Yang,Gregory Harris,Michael Griffin,Phyllis Small,Vanessa Hall,Luke Salazar,Jonathan Mcbride,Lindsey Smith,Lori Porter,James Haney,Cynthia Higgins,Gina Mata,Eric Nixon,Steven Flores,Jeremy Sheppard,Martin Kane,Rickey Campbell,Jessica Molina,Ronald Hernandez,Melissa Chen,Maxwell Anderson,Karen Fox,Eileen Bailey,Nina Smith,David Watson,Austin Guerrero,Laura Johnson,David Powell,Elizabeth Kim,Gabriel Smith,Monica Robinson,Jodi Rosales,Sarah Cole,Steven Ayala,Paul Long,Deborah Owens,Michelle Chapman,Alex Jacobson,Jessica Davis,Gloria Odom,Michelle Andrews,Christine James,James Barker,John Gray,Gina Sosa,James Miller,Elizabeth Fuentes,Bradley Mitchell,Brian Brooks,Katherine Jenkins,Chad Norton,Michael Bradford,Gregory Dominguez,Regina Johnson,Emily Jimenez,Ryan Roberts,Patrick Coleman,Kathleen Buck,Angela Martin DVM,Monica Cordova,Christopher Neal,Cindy Spears,Jonathan Bridges,Joseph Rivera,Brian Hinton,Jason Chung,Jeffery Rodriguez,Dustin Garcia,William Barrett,Andrew Barnes,Vincent Anderson,Joseph Gonzales,David Terry,Alex Sullivan,Cody Moody,Jake Stevens,Carolyn Robinson,Anna Baker,Gary Flowers,Larry Washington,Rebecca Carey,Linda Espinoza MD,Anthony Jordan,Kimberly Morris,Kurt Williams,Jacob Gilbert,Laura Reed,Cody Hampton,Vincent Green,Pam Mayo,Amanda Brown,Matthew Mclaughlin,Chris Larsen,Shawn Santiago,Amanda White,James Brown,Donald Campos,Michael Berger,Lisa Cameron,Thomas Newton,Kathy Brown,Cindy Perez,Dustin Kelley,Dalton Hensley DVM,Jason Travis,David Miller,Chad Payne,Carla Hernandez,Kelly Brennan,William Snyder,Justin Reed,Andrew Morris,Jason Lawrence,Robert Gomez,Dustin Little,Alexander Clark,Alexandra Mitchell,Laura Fernandez,Mary Butler,Austin Norton,Jennifer King,Randall Austin,Ellen Barrett,Carrie David,David Lyons,Brian Owens,Erik Wilcox,Jeremy Lowery,Tammy Cain,Annette Mcmahon,Marcia Sanchez DDS,Jonathan Evans,Amber Walker,Mark Bailey,Phillip Williams,Joann Fritz,Nicole Osborne,Deanna Bates,Amanda Lynch,Kyle Chambers,Phillip Shaffer,Sandra Kelly,Charles Romero,Sean Cruz,David Reilly,Joseph Oconnor,William Thomas,Melissa Schultz,Christopher Hamilton,Julian Keller,Alec Roy,Suzanne Sanchez,Faith Harris,Stephanie Price,Jessica Johnson,Nathan Mullins,Robin Alexander,Christian Warner,James Martinez,Charles Grant,Christopher Ford,Austin Jones,Michelle Robertson,Justin Whitehead,Mr. Larry Dillon,Lindsay Smith,Monica Irwin,Gary Perkins,Christopher Rice,Ricardo Smith,Steven Hatfield,Deborah Mcdonald,Robert Gibbs,Heather Tucker,James Schultz,Jordan Ramos,Lindsey Taylor,Brittney Thomas,Edward Hansen,Cindy Marsh,Megan Johnson,Joyce Leon,Mary Flynn,Vanessa Allen,James Jensen,Laura Merritt,Jeffrey Blanchard,Melissa Kim,Megan Johnson,Brad Stanton,Cynthia Harvey,Mark Morgan,Brian Hicks,Kristina Johnson,Destiny Lloyd,Jamie Cochran,Charles Gillespie,Aaron Henderson,Brandy Wilkinson,Frederick Lee,Denise Garcia,David Mcintyre,Andrew Young,Kara Sullivan,Jennifer Perez,Monique Rivera,Mark Gilmore,Michelle Hill,Henry Barton,Ryan Avery,Danielle Dominguez,Jose Burton,Jamie Oliver,Paula Reed,Rachel Cooper,Aaron Lin,Adam French,Jasmine Fox,Kimberly Collins,Robin Bridges,Jenna Murphy,Jennifer Schmidt,Nancy Turner,Jeffrey Williams,Marco Mcneil,Kayla Guzman,Dwayne Holloway,Ryan Brown,Diana Coleman,William Norris DVM,Gabriella Cross,Natasha Palmer,Diane Pittman,Jason Sanchez,Matthew Reid,Cameron Coleman,Eric Rubio,Paul Young,Denise Mills,Tiffany Torres,Elizabeth Schneider,Aaron Johnston,Colton Navarro,Tara Moore,Michael Rivera,John Perez,Monica Harris,Paul Taylor,Taylor Griffin,Laurie Evans,John Castillo,Kaitlyn Owen,James Johnson,Autumn Mcconnell,Carlos Kim,Terri Castillo,Vickie Thompson,Gary Scott,Ashley Diaz,Mitchell Perez,Donald Reed,Melissa Perez,Cynthia Brown,Debra Taylor,Charles Lambert,Mario Saunders,Jessica Perez,James Davis,John Smith,Richard Vargas,Amanda Erickson,Jesus Leblanc Jr.,Zachary Mason,Stephanie Sullivan,Jeremiah Rice,Peter Hunter,Steven Richard,Katherine Gonzalez,Brittany Bridges,Gabriel Mayer,Daniel Diaz,Emma Gonzales,Peter Young,Christopher Campbell IV,Jennifer Johnson,Adrian Bush,Danielle Pena,Tara Luna,Susan Thompson,Christopher Blake,Tina Mccall,Tyrone Smith,Wayne Daniels,Dr. Kevin Barker,Alexander Hall,Dennis Lewis,Marie Velasquez,Ashley Page,Brooke Liu,Jennifer Mata,Angela Dorsey,Rebecca Nguyen,Felicia Cross,Paige Hunter,Teresa Huynh,Jamie Riley,Kenneth Cuevas,Lauren Mcintosh,Michael Durham,Shane Hurst,Brian Gibson,Jason Todd,Mr. Joseph Anderson,Beth Baker,Cameron Hubbard,Ashley Fitzpatrick,Kevin Clark,Dr. Christopher Price,Chris Davis,Kristi Lynch,Antonio Taylor,Erika Rojas,Philip Allen,Kristina Powell,Douglas Dunn,Erin Webb,Derek Reed,Connie Baker,Robert Baker,Megan Jones,Richard Holt,Angie Cardenas,Douglas Burton,Mrs. Shannon Martin,Amy Wilson,William Garcia,Jennifer Coleman,Nancy Johnson,Karen Nicholson,James Richardson,Alexa Hill,Daniel Hobbs,Jeanette Shaw,Miranda Malone,Kayla Collier,Patricia Andrews,Emily Arnold,Veronica Brown,Jeff Goodman,Mark Stevens,James Brown,Kelly Jones,William King,Karen Moon,Daniel Hernandez MD,Veronica Schultz,Stephanie Zamora,Jamie Lopez,James Avila,Tiffany Ramirez,Guy Burnett,James Cooper,Daniel Pena,Christine Weaver,Jennifer Erickson,Brent Moore,Brittany Dean,Sophia Wong,Cory Smith,Ashley Rogers,Jennifer Johnson,Carolyn Miller,Janet Daniel,Carrie Hines,Brenda Morrow DDS,Suzanne Park,Emma Morgan,Kevin Garcia,Erin Richard,Bonnie Grant,David Bradshaw,Matthew Lewis,Julian Mora,Mrs. Carol Garrett,Michael Murphy,Darryl Ayers,Jordan Wilkerson,Vanessa Martinez,Hannah Flores,Ms. Nancy Maldonado,Natasha Cochran,Andre Mccullough,Todd Day,Melanie Knight,David Jackson,Robert Alexander,Susan Anderson MD,Karen Thompson,Alexandra Hudson,Sean Austin,Jeanette Gomez,Nancy Black,Angela Gibbs,Joseph Williams,Debra Harris,Amanda Martin,Jeffrey Hansen,Jason Olsen,Brandy Reid,Daniel Humphrey,Melissa Lang,Jerry Long,Joel Nelson,Anita Vaughan,Richard Walker,Thomas Wilson,Sean Garcia,Sabrina Jones,William Mcdonald,Joseph Daugherty,Joshua Rodriguez,Aaron Weaver,Kristen Armstrong,Timothy Ray,William Fox,Allison Garcia,Andrea Bowman,Derrick Hopkins,Reginald Gonzalez,Briana Harris,Adam Anderson,Sean Vincent MD,Raymond Barnes,John Miranda,Lisa Smith,Richard Daniels,Terry Rogers,Alexandria Benson,Mark Novak,Jared Munoz,David Harvey,Andrew White,David Conley,Kim Richmond,Christopher Bailey,Christopher Hull,Earl Harris,Jonathan Byrd,Matthew Thompson,Kiara Hughes,Victor Mcdonald,Isaiah Moses,Andrew Moore,Robert Perry,William Patterson,Caleb Hines,Gerald Whitehead,Dr. Lisa Cunningham,Nicholas Strickland,Jon Patel,Monica Stanley,Tyler Campbell,Janet George,Madison Brooks,Martha Rodriguez,Shane Leonard,Brad Craig,Kathryn Bell,Travis Vasquez,Kari Garcia,Christopher Williams,Elizabeth Davis,Katelyn Whitehead,Christie Brown,Keith Henry,Benjamin Ramos,Mark Arnold,Robin Ortiz DDS,Megan Hamilton,Alyssa Mcguire,Debra Velasquez,Chelsea Burgess,Krystal Mcgrath,Brian Kramer,Heather Simpson,Lauren Hernandez,Mr. Aaron Meza,Courtney Henry,Fernando Brown,Lisa Banks,Barbara Curtis,Kelly White,Johnny Brown,David Lucas,Sarah Martin,Betty Patrick,Sarah Ramirez,Christina Black,Jennifer Diaz,Joshua Nash,Stephen Schneider,David Johnson,Kellie Spears,Allison Carlson,Juan Becker,Ryan Young,Ashley Johnson,Steven Lee,Shawn Miller,Jeffrey Stewart,David Taylor,Vanessa Cervantes,Mr. Johnny Gonzalez,Beth Salazar,Dr. Taylor Holder,Elizabeth Johnson,Debra Rodriguez,Connor Mosley,James Bradley,Gilbert Avery,Lucas Cox,Melissa Hamilton,Andrew Floyd,Kathryn Brown,Elizabeth King MD,Timothy Hamilton,John Lopez DVM,Sarah Baker,Beth Perkins,Mark Warner,John Martinez,Michelle Stanley,Matthew Collins,Brandon Jones,Ricardo King,Mary Velazquez,Terry Garcia,Susan Lopez,Wanda Macias,Mike Stephenson,Thomas Brown,Steven Harris,Gerald Lambert,Joanne Young,Brandi Shaw,Alexis Forbes,Courtney Myers,Alexander Ochoa,Anthony Smith,James Alvarado,Heidi Richards,Greg Jackson,Christopher Lopez,Mark Hall,Shelly Woods,Amanda Torres,Sandra Dennis,Madison Martin,Anthony Coffey,Calvin Hale,Kayla Sloan,Leah Sullivan,William Ross,Randall Garcia,Angela Espinoza MD,Daniel Adams,Valerie Owen,Sarah Perez,Megan Rollins,Beverly Black,Rhonda Logan,Kylie Butler,Allison Wright,Karen Henderson,Meredith Johnson,Jonathon Jones,Rebecca Alexander,Kathleen Forbes,Frances Lang,Francisco Guerrero,Jessica Hughes"
    list = names.split(",")

    query = "CREATE "
    for i in range(0, len(list)-900):
        query += "(a" + str(i) + ":Person{taxCode:'" + str(i + 4) + "', name:'" + list[i].split(' ')[
            0] + "',surname:'" + list[i].split(' ')[1] + "'})"
        if i < len(list) - 900 - 1:
            query += ', '

    print("computing query...")
    conn.compute_query(query)
    print("people created")


def compute_families(list_to_copy):

    list = list_to_copy.copy()

    current_family = []

    for numF in range(1, randint(7, 15)):
        query = ""
        numM = randint(2, 5)
        for i in range(0, numM):
            current_family.append(list.pop(randint(0, len(list) - 1)))
        print("current family: " + str(current_family))
        for a in current_family:
            for b in current_family:
                if a != b:
                    print("adding a family to the query...\n")
                    query += "MATCH "
                    query += "(a: Person), (b: Person) WHERE a.taxCode = '" + str(a) + "' AND b.taxCode = '" + str(
                        b) + "' CREATE (a)-[r:FAMILY_CONNECTION]->(b) WITH 1 AS dummy\n"
        query += "return 1"

        current_family.clear()
        print("computing query...")
        conn.compute_query(query)
        print("families done.")


def compute_places_and_connections(list):
    places = ["Restaurant Alfredo", "Restaurant Bella Napoli", "Magic Restaurant", "Sushi bar", "Sexy shop",
              "Cocktail bar", "Library", "University", "Supermarket", "Bakery", "Ikea", "Duomo of Milan", "Stadium",
              "Theatre", "Grocery shop", "Church"]
    sources = ["Immuni", "By manual registration"]
    for p in places:
        conn.compute_query("CREATE (:Place {name: '"+p+"'})")

    places.append("Casual meeting on the street")
    places.append("Casual meeting on the street")
    places.append("Casual meeting on the street")
    places.append("Casual meeting on the street")

    today = date.today()

    for n in range(randint(40, 60)):
        p1 = str(randint(min(list), max(list)))
        p2 = str(randint(min(list), max(list)))

        # Dates of meetings can vary of +-2 months (in the two worst cases) from the current date
        year = today.year
        month = today.month + randint(-1, 1)
        day = randint(1, 28)

        hour = randint(0, 22)
        minute = randint(0, 59)
        datetime1 = "datetime('" + str(year) + "-" + str(month) + "-" + str(day) + "T" + str(hour) + ":" + str(minute) + "')"
        place = places[randint(0, len(places) - 1)]
        if place == "Casual meeting on the street":
            a = randint(1, 10)
            datetime2 = datetime1
            source = "Immuni"
            p2 = str(randint(min(list), max(list)))
            if a % 2 == 0:
                query = " CREATE (p:Place {name: \"Casual meeting on the street at " + datetime1 + "\"}) WITH 1 as dummy MATCH (a:Person),(b:Person),(p:Place) WHERE a.taxCode = '" + p1 + "' AND b.taxCode = '" + p2 + "' AND p.name = \"Casual meeting on the street at " + datetime1 + "\" CREATE (a)-[:WENT_TO{entry_moment:" + datetime1 + ",exit_moment:" + datetime2 + ", source:'" + source + "'}]->(p) CREATE (b)-[:WENT_TO{entry_moment:" + datetime1 + ",exit_moment:" + datetime2 + ", source:'" + source + "'}]->(p) CREATE (p)-[:HOSTED{entry_moment:" + datetime1 + ",exit_moment:" + datetime2 + ", source:'" + source + "'}]->(a) CREATE (p)-[:HOSTED{entry_moment:" + datetime1 + ",exit_moment:" + datetime2 + ", source:'" + source + "'}]->(b)"
            else:
                p3 = str(randint(min(list), max(list)))
                query = " CREATE (p:Place {name: \"Casual meeting on the street at " + datetime1 + "\"}) WITH 1 as dummy MATCH (a:Person),(b:Person),(c:Person),(p:Place) WHERE a.taxCode = '" + p1 + "' AND b.taxCode = '" + p2 + "' AND c.taxCode = '" + p3 + "' AND p.name = \"Casual meeting on the street at " + datetime1 + "\" CREATE (a)-[:WENT_TO{entry_moment:" + datetime1 + ", exit_moment:" + datetime2 + ", source:'" + source + "'}]->(p) CREATE (b)-[:WENT_TO{entry_moment:" + datetime1 + ", exit_moment:" + datetime2 + ", source:'" + source + "'}]->(p) CREATE (c)-[:WENT_TO{entry_moment:" + datetime1 + ", exit_moment:" + datetime2 + ", source:'" + source + "'}]->(p) CREATE (p)-[:HOSTED{entry_moment:" + datetime1 + ", exit_moment:" + datetime2 + ", source:'" + source + "'}]->(a) CREATE (p)-[:HOSTED{entry_moment:" + datetime1 + ", exit_moment:" + datetime2 + ", source:'" + source + "'}]->(b) CREATE (p)-[:HOSTED{entry_moment:" + datetime1 + ", exit_moment:" + datetime2 + ", source:'" + source + "'}]->(c)"

        else:
            datetime2 = "datetime('" + str(year) + "-" + str(month) + "-" + str(day) + "T" + str(randint(hour + 1, 23)) + ":" + str(randint(0, 59)) + "')"
            source = sources[randint(0, len(sources) - 1)]

            if randint(0, 100) < 50:
                query = " MATCH (a:Person),(b:Person),(p:Place) WHERE a.taxCode= '" + p1 + "'and b.taxCode = '" + p2 + "' and p.name='" + place + "' CREATE (a)-[:WENT_TO{entry_moment:" + datetime1 + ",exit_moment:" + datetime2 + ", source:'" + source + "'}]->(p) CREATE (p)-[:HOSTED{entry_moment:" + datetime1 + ",exit_moment:" + datetime2 + ", source:'" + source + "'}]->(a) CREATE (b)-[:WENT_TO{entry_moment:" + datetime1 + ",exit_moment:" + datetime2 + ", source:'" + source + "'}]->(p) CREATE (p)-[:HOSTED{entry_moment:" + datetime1 + ",exit_moment:" + datetime2 + ", source:'" + source + "'}]->(b) WITH 1 AS dummy "
            else:
                query = " MATCH (a:Person),(b:Place) WHERE a.taxCode='" + p1 + "' and b.name='" + place + "' CREATE (a)-[:WENT_TO{entry_moment:" + datetime1 + ",exit_moment:" + datetime2 + ", source:'" + source + "'}]->(b) CREATE (b)-[:HOSTED{entry_moment:" + datetime1 + ",exit_moment:" + datetime2 + ", source:'" + source + "'}]->(a) WITH 1 AS dummy "

        query += "RETURN 1"
        conn.compute_query(query)


if __name__ == '__main__':
    conn = Connection("bolt://54.205.87.249:7687", "neo4j", "blankets-ride-firefighting")

    # Creation of a list of taxCodes, used to generate people, families, connections, ...
    tax_list = []
    for i in range(1, 101):
        tax_list.append(i)

    compute_people()

    compute_gp(tax_list)

    compute_families(tax_list)

    compute_infections(tax_list)

    compute_places_and_connections(tax_list)

    conn.close()
