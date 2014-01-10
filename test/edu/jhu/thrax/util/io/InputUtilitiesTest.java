package edu.jhu.thrax.util.io;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.jhu.thrax.util.exceptions.MalformedInputException;

public class InputUtilitiesTest
{
    @Test
    public void parseYield_EmptyString_ReturnsZeroLengthArray() throws MalformedInputException
    {
        Assert.assertEquals(InputUtilities.parseYield("").length, 0);
    }

    @Test
    public void parseYield_Whitespace_ReturnsZeroLengthArray() throws MalformedInputException
    {
        Assert.assertEquals(InputUtilities.parseYield("        ").length, 0);
    }
    @Test
    public void parseYield_EmptyParse_ReturnsZeroLengthArray() throws MalformedInputException
    {
        Assert.assertEquals(InputUtilities.parseYield("()").length, 0);
    }

    @Test(expectedExceptions = { MalformedInputException.class })
    public void parseYield_UnbalancedLeft_ThrowsException() throws MalformedInputException
    {
        InputUtilities.parseYield("(S (DT the) (NP dog)");
    }

    @Test(expectedExceptions = { MalformedInputException.class })
    public void parseYield_UnbalancedRight_ThrowsException() throws MalformedInputException
    {
        InputUtilities.parseYield("(S (DT the) (NP dog)))");
    }
}

