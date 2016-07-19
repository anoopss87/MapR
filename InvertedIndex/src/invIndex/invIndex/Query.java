package invIndex.invIndex;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;

public class Query
{
	private static Map<String, ArrayList<String>> queryMap = new HashMap<String, ArrayList<String>>();
	public static void buildTable(String filePath) throws IOException
	{
		BufferedReader br = null;
		try
		{
			String line;
			br = new BufferedReader(new FileReader(filePath));

			while ((line = br.readLine()) != null)
			{
				String words[] = line.split("\t");
				String res[] = words[1].split("`");
				ArrayList<String> temp = new ArrayList<String>();
				for(int i=0;i<res.length;++i)
				{
					if(i == 5)
						break;
					String[] entry = res[i].split(":");
					temp.add(entry[0]);					
				}
				queryMap.put(words[0], temp);
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			br.close();
		}
	}
	public static void main(String[] args) throws IOException
	{
		String filePath = System.getProperty("user.dir") + "\\output.txt";
		buildTable(filePath);
		
		try
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));				
						
			while(true)
			{
				System.out.println("Enter the query :");
				String searchLine = br.readLine();
				
				if(searchLine.equals("!exit!"))
				{
					System.out.println("Exiting!!!!!!!!");
					System.exit(0);
				}
				String[] str = searchLine.split(" ");
				ArrayList<HashSet<String>> hsList = new ArrayList<HashSet<String>>();
				LinkedHashSet<String> res = new LinkedHashSet<String>();
				LinkedHashSet<String> foundStr = new LinkedHashSet<String>();
				boolean skip = false;
				for(String search : str)
				{
					if(queryMap.containsKey(search))
					{						
						HashSet<String> temp = new HashSet<String>();
						foundStr.add(search);
						for(String s : queryMap.get(search))
						{														
							temp.add(s);
						}
						hsList.add(temp);						
					}
					else
					{
						System.out.println("No results found for the query \"" + searchLine + "\"");
						skip = true;
						break;
					}
				}
				
				if(skip)
					continue;
				for(String search : foundStr)
				{
					for(String s : queryMap.get(search))
					{
						boolean intersect = true;
						for(HashSet<String> hs : hsList)
						{
							if(!hs.contains(s))
							{
								intersect = false;
								break;
							}
						}
						if(intersect)
						{
							res.add(s);
						}
					}
				}
				if(res.isEmpty())
				{
					System.out.println("No results found for the query \"" + searchLine + "\"");
				}
				else
				{
					System.out.println("The top results for the query \"" + searchLine + "\" are:");
					for(String s : res)
						System.out.println(s);
				}
			}				
		}
		catch(IOException io)
		{
			io.printStackTrace();
		}		
	}
}